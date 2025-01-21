import requests
import pandas as pd
import os
from glob import glob
import re
import threading
import concurrent.futures
from tqdm import tqdm
import numpy as np

from collect.util import load_api_key


class KakaoAPIManager:
    def __init__(
        self,
        api_key_path='api_keys.txt',
        target_path='./data/data.tsv',
        guso_path='./data/API_카카오_시군구동_20241217.xlsx',
        progress_file_path="./save_data/",
    ):
        self._load_data(api_key_path, target_path, guso_path)
        self.api_index = 0
        self.headers = {"Authorization": f"KakaoAK {self.api_keys[self.api_index]}"}
        self.counts = 0
        self.progress_file_path = progress_file_path

        self.api_key_lock = threading.Lock()
        self.save_lock = threading.Lock()

        self.saved_progress = self._load_progress()

    def _load_data(self, api_key_path, target_path, guso_path):
        self.api_keys = load_api_key(api_key_path)
        self.target_df = pd.read_csv(target_path, sep='\t')

        guso = pd.read_excel(guso_path)
        self.guso_index = {}
        for i, row in guso.iterrows():
            gu = row['SIGUNGU_NM'] + '' if type(row['GU_NM']) != str else ' ' + row['GU_NM']
            dong = row['DONG_NM']
            sido = row['SIDO_NM']
            if gu in self.guso_index:
                self.guso_index[gu]['dong'].append(f"{sido} {gu} {dong}")
            else:
                self.guso_index[gu] = {
                    'only_gu': row['SIGUNGU_NM'] if type(row['GU_NM']) != str else ' ' + row['GU_NM'],
                    'gu': f"{sido} {gu}",
                    'dong': [f"{sido} {gu} {dong}"]
                }

    def _name_change(self, name):
        filtered_text = re.sub(r"[^가-힣0-9a-zA-Z]", "", name)
        return filtered_text

    def _save_progress(self, data, store_name):
        """
        수집한 데이터를 진행 상황에 저장.
        """
        save_path = os.path.join(self.progress_file_path, store_name + '.csv')

        with self.save_lock:
            if not os.path.exists(save_path):
                os.makedirs(self.progress_file_path, exist_ok=True)
                data.to_csv(save_path, index=False, encoding="utf-8-sig")
            else:
                try:
                    existing_data = pd.read_csv(save_path)
                    updated_data = pd.concat([existing_data, data]).drop_duplicates(['id'])
                    updated_data.to_csv(save_path, index=False, encoding="utf-8-sig")
                except Exception as e:
                    # 깨진 파일인 경우 overwrite
                    data.to_csv(save_path, index=False, encoding="utf-8-sig")

    def _load_progress(self):
        """
        저장된 진행 상황 로드 -> 이미 처리 완료된 store_name들의 리스트를 반환
        """
        save_path_list = glob(f"{self.progress_file_path}/*.csv")
        progressed = [os.path.basename(i).rstrip('.csv') for i in save_path_list]
        return progressed

    def rotate_api_key(self):
        """
        API Key를 순환하며 소진 시 다음 키로 변경.
        여러 스레드에서 동시에 접근할 수 있으므로 Lock
        """
        with self.api_key_lock:
            self.api_index += 1
            if self.api_index >= len(self.api_keys):
                # 987 예외를 발생시켜서 상위에서 중단하도록 유도
                raise Exception(987, "모든 API 키 소진")

            print(f"[API KEY ROTATE] {self.api_keys[self.api_index - 1]} -> {self.api_keys[self.api_index]}")
            self.headers = {"Authorization": f"KakaoAK {self.api_keys[self.api_index]}"}

    def get_places(self, keyword, page=1):
        """
        Kakao 로컬 검색 API를 호출하여 데이터를 가져옴.
        """
        url = "https://dapi.kakao.com/v2/local/search/keyword.json"
        params = {"query": keyword, "page": page}
        try:
            response = requests.get(url, params=params, headers=self.headers)
            self.counts += 1
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429 or response.text == '{"errorType":"RequestThrottled","message":"API limit has been exceeded."}':
                # API Limit 초과로 인해 인증 실패 시 -> 키 변경
                self.rotate_api_key()
                return self.get_places(keyword, page)
            else:
                print(f"[Error {response.status_code}]: {response.text}")
                raise
        except Exception as e:
            # 여기서 987 예외는 그대로 재발생시켜 상위로 전달
            if len(e.args) > 0 and e.args[0] == 987:
                raise
            print(f"[API 요청 실패] {e}")
            raise

    def check_stop(self, result, gu_name=None):
        """
        수집 중단 조건 확인.
        """
        if not result or 'documents' not in result:
            return True
        docs = result['documents']
        if len(docs) < 15:
            return True
        if gu_name:
            last_addr = docs[-1]['address_name']
            if gu_name not in last_addr:
                return True
        return False

    def collect_stores_in_parallel(self, max_workers=10):
        """
        [병렬 버전 + tqdm]
        영업표지 기준으로 데이터를 수집하고 진행 상황을 저장/이어가기.
        최대 10개씩(기본값) 스레드로 동시에 처리하며, 진행 바를 표시.
        """
        completed_stores = set(self.saved_progress)
        total_target_count = len(self.target_df)

        tasks = []
        for target_index, row in self.target_df.iterrows():
            raw_name = row["영업표지"]
            store_name = self._name_change(raw_name)
            if store_name not in completed_stores:
                tasks.append((target_index, store_name))

        print(f"[INFO] 총 수집 대상: {len(tasks)} / 전체 {total_target_count}")

        with tqdm(total=len(tasks), desc="전체 진행 상황") as pbar:
            # ThreadPoolExecutor 실행
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_store = {
                    executor.submit(self._collect_and_save_store, store_name, idx, total_target_count): (store_name, idx)
                    for idx, store_name in tasks
                }

                try:
                    for future in concurrent.futures.as_completed(future_to_store):
                        store_name, idx = future_to_store[future]
                        try:
                            result = future.result()
                        except Exception as exc:
                            # 모든 API키 소진 에러(987) 발생 시
                            if len(exc.args) > 0 and exc.args[0] == 987:
                                print("[ERROR] 모든 API 키 소진: 나머지 작업을 중단합니다.")
                                # 스레드 풀 강제 종료 (대기 중인 작업 취소)
                                executor.shutdown(wait=False, cancel_futures=True)
                                break
                            else:
                                print(f"[ERROR] {store_name} (index:{idx}) 수집 실패: {exc}")
                        else:
                            # 정상 수행 결과에 대한 처리
                            pass
                        finally:
                            pbar.update(1)

                except Exception as e:
                    print(f"[ERROR] 메인 루프에서 처리되지 않은 에러: {e}")

        print("[INFO] 모든 스레드 작업 완료.")

    def _collect_and_save_store(self, store_name, target_index, total_target_count):
        """
        개별 스레드에서 실행되는 함수.
        """
        try:
            print(f"[START] {store_name} (index:{target_index}/{total_target_count}) 수집 시작")
            result_df = self.collect_stores(store_name)
            rd_len = len(result_df)
            result_df.drop_duplicates(['id'], inplace=True)
            print(f"[DE-DUP] {store_name} 중복 제거 {rd_len} -> {len(result_df)}")

            self._save_progress(result_df, store_name)
            print(f"[SAVED] {store_name} - 데이터 저장 완료")
            return result_df

        except Exception as e:
            # 987 예외(모든 api key 소진)는 그대로 raise
            if len(e.args) > 0 and e.args[0] == 987:
                raise
            # 그 외 에러는 여기서 처리 가능 (로그 남기기 등)
            raise

    def collect_stores(self, store_name):
        """
        특정 가게 이름을 기준으로 데이터를 수집.
        """
        first_result = self.get_places(store_name, page=1)
        if not first_result or 'meta' not in first_result:
            print(f"[{store_name}] 첫 검색 결과가 없거나 오류.")
            return pd.DataFrame()

        total_count = first_result['meta']['total_count']
        print(f"[{store_name}] total_count = {total_count}")
        all_collected = []

        if total_count <= 15:
            all_collected.append(first_result)
        elif total_count <= 45:
            print(f" => 45건 이하, 단순 전체 검색 (1~3페이지)")
            results = self.collect_data_by_region(store_name, '', start_page=2, max_pages=3)
            results = [first_result] + results
            all_collected.extend(results)
        else:
            print(f" => 45건 초과, 구, 동 단위 검색")
            for gu, value in self.guso_index.items():
                gu_keyword = value['gu']
                only_gu = value['only_gu']
                first_result = self.get_places(store_name + ' ' + gu_keyword, page=1)

                if first_result['meta']['total_count'] <= 45:
                    gu_results = self.collect_data_by_region(store_name, gu_keyword, gu_name=only_gu, start_page=2, max_pages=3)
                    all_collected.extend([first_result] + gu_results)
                else:
                    for dong_keyword in value['dong']:
                        dong_results = self.collect_data_by_region(store_name, dong_keyword, gu_name=only_gu, max_pages=3)
                        if dong_results:
                            all_collected.extend(dong_results)

        all_collected_df = self.data_transform(all_collected)
        return all_collected_df

    def data_transform(self, all_collected_list):
        """
        여러 JSON 응답을 DataFrame으로 변환
        """
        new_data = []
        for res_json in all_collected_list:
            if len(res_json.get('documents', [])) == 0:
                continue
            keyword = res_json['meta']['same_name']['keyword']
            for d in res_json['documents']:
                d['keyword'] = keyword
                new_data.append(d)
        df = pd.DataFrame(new_data)
        return df

    def collect_data_by_region(self, keyword, region, gu_name=None, start_page=1, max_pages=3):
        """
        특정 지역 + 키워드를 가지고, 최대 max_pages까지 반복하여 검색
        """
        all_results = []
        for page in range(start_page, max_pages + 1):
            combined_keyword = f"{keyword} {region}"
            result = self.get_places(combined_keyword, page=page)
            if not result or 'documents' not in result:
                break
            all_results.append(result)
            if self.check_stop(result, gu_name):
                break
        return all_results


if __name__ == "__main__":
    manager = KakaoAPIManager()
    manager.collect_stores_in_parallel(max_workers=10)
