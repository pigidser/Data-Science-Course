import requests
if __name__ == "__main__":
    r = requests.post('http://localhost:5000/add', json={'num':15})
    # print(r.status_code, r.json())
    if r.status_code == 200:
        print(r.status_code,r.json()['result'])
    else:
        print(r.status_code,r.text)