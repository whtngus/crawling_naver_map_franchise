# from collect.kakao_api import KakaoAPIManager
#
#
# kakao_api = KakaoAPIManager(progress_file_path='./save_data/')
# kakao_api.collect_stores_with_resume()
#

from collect.kakao_api_m import KakaoAPIManager
from time import time

while True:
    kakao_api = KakaoAPIManager(progress_file_path='./save_data/')
    kakao_api.collect_stores_in_parallel(max_workers=10)
    time.sleep(3600*23)