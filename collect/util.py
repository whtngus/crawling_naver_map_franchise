import requests

def load_api_key(path = 'api_keys.txt'):
    with open(path, 'r') as f:
        api_keys = [i.strip() for i in f.readlines()]
    return api_keys


def api_key_check():
    api_keys = load_api_key()

    for api_key in api_keys:
        headers = {"Authorization": f"KakaoAK {api_key}"}
        url = "https://dapi.kakao.com/v2/local/search/keyword.json"
        params = {"query": "강남역:", "page": 1}
        response = requests.get(url, params=params, headers=headers)
        print(f"key chcedk - ", api_key)
        print(response)
        print(response.text)