import requests
if __name__ == "__main__":
    r = requests.post('http://localhost:5000/predict', json={'num1':0.3,'num2':0.3,'num3':0.3,'num4':0.3})
    if r.status_code == 200:
        # print(r.status_code,r.json()['result'])
        print('{'+r.text+'}')
    else:
        print(r.status_code,r.text)