from collect.kakao_api import KakaoAPIManager
import time
from collect.util import  api_key_check

# api key 등록시 정상적으로 넘어오는지 체크하는 코드
# api_key_check()

while True:
    kakao_api = KakaoAPIManager(progress_file_path='./save_data/')
    kakao_api.collect_stores_in_parallel(max_workers=10)
    time.sleep(3600*23)





