from flask import Flask, request, jsonify
from datetime import datetime

app = Flask(__name__)

@app.route('/hello')
def print_hello():
    name = request.args.get('name')
    return(f'Hello, {name}')

@app.route('/time')
def get_current_time():
    return(f'Current time is {datetime.now()}')

@app.route('/add',methods=['POST'])
def add():
    num = request.json.get('num')
    if num > 10:
        return 'too_much', 400
    return(jsonify({'result':num+1}))

@app.route('/predict',methods=['POST'])
def predict():

    import pika
    import pickle
    import numpy as np
    import json
    import os

    dir_path = os.path.dirname(os.path.realpath(__file__))

    with open(dir_path+'/hw1.pkl', 'rb') as pkl_file:
        regressor = pickle.load(pkl_file)

    num1 = request.json.get('num1')
    num2 = request.json.get('num2')
    num3 = request.json.get('num3')
    num4 = request.json.get('num4')
    features = np.reshape(np.array([num1,num2,num3,num4]),(1,-1))
 
    y_pred = regressor.predict(features)
    return f'"prediction": {y_pred[0]}'
    # return(jsonify({'prediction':list(y_pred)}))

if __name__ == "__main__":
    app.run('localhost',5000)