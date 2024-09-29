import os
import json
import boto3
from flask import Flask, jsonify, request
import mlflow
import pandas as pd
from pandas.tseries.offsets import DateOffset

# import mlflow
# import sys
# from airflow.hooks.postgres_hook import PostgresHook
# from prophet import Prophet
# from prophet.diagnostics import cross_validation
# from prophet.diagnostics import performance_metrics


# s3 = boto3.resource('s3')

# bucket = s3.Bucket('prueba-15-sept-mlops')

# key = '/1/1b44b22a64f84d95a31779c3d8fb3adb/artifacts/prophet_2/'
# objs = list(bucket.objects.filter(Prefix=key))

# for obj in objs:
#     #print(obj.key)
#     out_name = obj.key.split('/')[-1]
#     bucket.download_file(obj.key, out_name)  


app = Flask(__name__)

# client = boto3.client('s3')
# bucket = "prueba-15-sept-mlops"
# cur_path = os.getcwd()


@app.route("/predict", methods=['GET', 'POST'])
def predict():
    month = json.loads(request.args["month"])
    offset = json.loads(request.args["offset"])
    df = pd.DataFrame({
    'ds': pd.date_range(
        start = pd.Timestamp(month),                        
        end = pd.Timestamp(month) + DateOffset(offset), 
        freq = 'D'
         )
    })
    model_uri = "./prophet"
    model = mlflow.pyfunc.load_model(model_uri)
    forecast = model.predict(df)
    result = forecast.to_json(orient='records')
    return jsonify(result)



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
