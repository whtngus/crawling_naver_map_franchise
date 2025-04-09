# 카카오 가맹점 데이터 수집 실행 설명서 

## 실행 방법

> 1. 패키지 설치
> pip insetall requirements.txt
> 2. 코드 실행
> python main.py
> 3. 결과 파일 압축 후 내부망으로 반입 
> main.py에  progress_file_path로 지정한 경로에 생긴 데이터를 수집 
> ``` kakao_api = KakaoAPIManager(progress_file_path='./save_data/') ```


## 사전 데이터 정비

### 1. 시동구 데이터

- 경로: /data/API_카카오_시군구동_20250117.xlsx

시 동 구 검색 대상이 있는 정보로 아래와 같은 형식의 데이터를 내부망에서 반입하여 저장 
단, 시 동 구 정부의 변동이 없는경우 기존 데이터를 그대로 사용

> 예시 데이터
> ``` SIDO_NM	SIGUNGU_NM	GU_NM	DONG_NM
> 경기	고양시	일산동구	장항1동
> 경기	안산시	단원구	신길동
> 경기	고양시	덕양구	흥도동
> 경기	성남시	분당구	서현1동
> 충북	청주시	상당구	낭성면
> ```

> 데이터 추출 방법 
> ```
SELECT DISTINCT
 GDS_WID_TRL_NM AS SICO_NM
 , GDS_DSR_NM AS SIGUNGU_NM
 , GDS_OL_NM AS GU_NM
 , ABN_NM AS DONG_NM
FROM SWOAE0019
```

- 새로운 경로에 데이터를 저장시 수정 방법
> main.py에서 guso_path="새로운데이터 경로" 를 변경한 경로로 수정 
> ``` kakao_api = KakaoAPIManager(progress_file_path='./save_data/', guso_path="새로운데이터 경로") ```

### 2. 가맹점 명 데이터

- 경로: /data/data.tsv

> 1. 링크 접속
> https://franchise.ftc.go.kr/mnu/00013/program/userRqst/list.do?column=brd&searchKeyword=&selUpjong=&selIndus=&pageUnit=20000
> 2. 링크내에 있는 모든 데이터 드래그 하여 복사 및 붙여넣기 하여 tsv 파일로 저장 

> 예시 데이터
> ```
> 번호	상호	영업표지	대표자	등록번호	최초등록일	업종
> 12437	국민부대찌개법원점	국민부대찌개	김성환	20200609	2020.05.18	한식
> 12436	(주)하루에프앤비	하루엔소쿠	한덕희	20160451	2016.05.16	일식
> 12435	명성F&B	명성가	박태우	20211634	2021.07.22	한식
> 12434	(주)홍락	미래회관	이승훈	20230139	2023.01.30	한식
> 12433	아사에프앤비(주)	아사(ASA)커피랩	김영진	20241640	2024.12.06	커피
> 12432	(주)컴퍼스에프앤비	하마네아구찜	오진형	20230686	2023.05.15	한식
> ```

