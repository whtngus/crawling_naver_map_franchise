import requests
from tqdm import tqdm
import pandas as pd
from collect.util import load_api_key
import os
from glob import glob
import re
class KakaoAPIManager:
    def __init__(self
                 , api_key_path='api_keys.txt'
                 , target_path='./data/data.tsv'
                 , guso_path = './data/API_카카오_시군구동_20241217.xlsx'
                 , progress_file_path="./save_data/",
        ):
        self._load_data(api_key_path, target_path, guso_path)
        self.api_index = 0
        self.headers = {"Authorization": f"KakaoAK {self.api_keys[self.api_index]}"}
        self.counts = 0
        self.progress_file_path = progress_file_path

        self.saved_progress = self._load_progress()

    def _load_data(self, api_key_path, target_path, guso_path):
        # gu_list / dong_list 예시
        # 지역 정보 불러오기 예시
        self.api_keys = load_api_key(api_key_path)
        self.target_df = pd.read_csv(target_path, sep='\t')
        guso = pd.read_excel(guso_path)
        self.gu_list = list(set([" ".join([j for j in i if type(j) == str])
                            for i in guso[['SIDO_NM', 'SIGUNGU_NM', 'GU_NM']].values]))
        self.only_gu_list = [i.split(' ')[-1] for i in self.gu_list]
        self.dong_list = [" ".join([j for j in i if type(j) == str])
                     for i in guso[['SIDO_NM', 'SIGUNGU_NM', 'GU_NM', 'DONG_NM']].values]
        self.only_gu_dong_list = list(guso['SIGUNGU_NM'])

    def _name_change(self, name):
        filtered_text = re.sub(r"[^가-힣0-9a-zA-Z]", "", name)
        return filtered_text
    def _save_progress(self, data, store_name):
        """
        수집한 데이터를 진행 상황에 저장.
        :param data: 저장할 데이터 (DataFrame)
        """
        save_path = os.path.join(self.progress_file_path, store_name + '.csv')
        if not os.path.exists(save_path):
            os.makedirs(self.progress_file_path,exist_ok=True)
            data.to_csv(save_path, index=False, encoding="utf-8-sig")
        else:
            existing_data = pd.read_csv(save_path)
            updated_data = pd.concat([existing_data, data]).drop_duplicates(['id'])
            updated_data.to_csv(save_path, index=False, encoding="utf-8-sig")
    def _load_progress(self):
        """
        저장된 진행 상황 로드.
        :return: 로드된 데이터 (DataFrame)
        """
        save_path = glob(f"{self.progress_file_path}/*.csv")
        progressed = [os.path.basename(i).rstrip('.csv') for i in save_path]
        return progressed

    def rotate_api_key(self):
        """
        API Key를 순환하며 소진 시 다음 키로 변경.
        """
        self.api_index += 1
        if self.api_index >= len(self.api_keys):
            raise Exception("모든 API 키 소진")
        print(f"api_key changed {self.api_keys[self.api_index-1]} -> {self.api_keys[self.api_index]}")
        self.headers = {"Authorization": f"KakaoAK {self.api_keys[self.api_index]}"}

    def get_places(self, keyword, page=1):
        """
        Kakao 로컬 검색 API를 호출하여 데이터를 가져옴.
        :param keyword: 검색 키워드
        :param page: 검색 페이지
        :return: API 응답 JSON
        """
        url = "https://dapi.kakao.com/v2/local/search/keyword.json"
        params = {"query": keyword, "page": page}
        try:
            response = requests.get(url, params=params, headers=self.headers)
            self.counts += 1
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:  # Unauthorized (API Key 만료)
                self.rotate_api_key()
                return self.get_places(keyword, page)
            else:
                print(f"[Error {response.status_code}]: {response.text}")
                return None
        except Exception as e:
            print(f"API 요청 실패: {e}")
            return None

    def check_stop(self, result, gu_name=None):
        """
        수집 중단 조건 확인.
        :param result: API 응답 결과
        :param gu_name: 구 이름
        :return: 중단 여부 (True/False)
        """
        if not result or 'documents' not in result:
            return True
        docs = result['documents']
        if len(docs) < 15:
            return True
        if gu_name and len(docs) > 0:
            last_addr = docs[-1]['address_name']
            if gu_name not in last_addr:
                return True
        return False

    def collect_stores_with_resume(self):
        """
        영업표지 기준으로 데이터를 수집하고 진행 상황을 저장/이어하기.
        """
        completed_stores = self.saved_progress
        total_target_count = len(self.target_df)
        for target_index, row in self.target_df.iterrows():
            store_name = row["영업표지"]
            store_name = self._name_change(store_name)
            if store_name in completed_stores:
                print(f"[SKIP] {store_name}: 이미 수집된 데이터")
                continue

            print(f"[START] {store_name}: 데이터 수집 시작 {target_index}/{total_target_count}")
            result_df = self.collect_stores(store_name)
            rd_len = len(result_df)
            result_df = result_df.drop_duplicates(['id'])
            print(f"[counts] 중복 제거 {rd_len} -> {len(result_df)}")
            # 진행 상황 저장
            self._save_progress(result_df, store_name)
            print(f"[SAVED] {store_name}: 데이터 저장 완료")
    def collect_stores(self, store_name):
        """
        특정 가게 이름을 기준으로 데이터를 수집.
        :param store_name: 가게 이름
        :return: 수집된 데이터 리스트
        """
        first_result = self.get_places(store_name, page=1)
        if not first_result or 'meta' not in first_result:
            print("첫 검색 결과가 없거나 오류.")
            return []

        total_count = first_result['meta']['total_count']
        print(f"[{store_name}] total_count = {total_count}")
        all_collected = []

        if total_count <= 15:
            all_collected.extend([first_result])
        elif total_count <= 45:
            print(" => 45건 이하, 단순 전체 검색 (1~3페이지)")
            results = self.collect_data_by_region(store_name, '', max_pages=3)
            all_collected.extend(results)
        elif total_count <= 1000:
            print(" => 1000건 이하, 구 단위 검색")
            for gu, only_gu in tqdm(zip(self.gu_list, self.only_gu_list), desc="구 단위 수집", total=len(self.gu_list)):
                gu_keyword = f"{gu} {store_name}"
                gu_results = self.collect_data_by_region(gu_keyword, only_gu, max_pages=3)
                all_collected.extend(gu_results)
        else:
            print(" => 1000건 초과, 동 단위 검색")
            for dong, only_gu in tqdm(zip(self.dong_list, self.only_gu_dong_list), desc="동 단위 수집", total=len(self.dong_list)):
                dong_keyword = f"{dong} {store_name}"
                dong_results = self.collect_data_by_region(dong_keyword, only_gu, max_pages=3)
                if dong_results:
                    all_collected.extend(dong_results)

        all_collected = self.data_transform(all_collected)
        return all_collected

    def data_transform(self, all_collected_list):
        new_data = []
        for all_collected in all_collected_list:
            if len(all_collected['documents']) == 0:
                continue
            keyward = all_collected['meta']['same_name']['keyword']
            for d in all_collected['documents']:
                d['keyword'] = keyward
                new_data.append(d)

        df = pd.DataFrame(new_data)
        return df


    def collect_data_by_region(self, keyword, region, gu_name='', max_pages=3):
        """
        특정 지역 및 키워드에 대한 데이터를 수집.
        :param keyword: 검색 키워드
        :param region: 검색 지역
        :param gu_name: 구 이름
        :param max_pages: 최대 페이지 수
        :return: 수집된 데이터 리스트
        """
        all_results = []
        for page in range(1, max_pages + 1):
            combined_keyword = f"{keyword} {region}"
            result = self.get_places(combined_keyword, page=page)
            if not result or 'documents' not in result:
                break
            all_results.append(result)
            if self.check_stop(result, gu_name):
                break
        return all_results

