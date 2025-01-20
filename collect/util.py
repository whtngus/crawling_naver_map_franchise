

def load_api_key(path = 'api_keys.txt'):
    with open(path, 'r') as f:
        api_keys = [i.strip() for i in f.readlines()]
    return api_keys