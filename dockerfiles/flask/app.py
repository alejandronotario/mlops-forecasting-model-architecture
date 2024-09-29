import os
import json
import boto3
from flask import Flask, jsonify, request, render_template
import mlflow
import pandas as pd
from pandas.tseries.offsets import DateOffset
from flask import Flask, render_template, request, redirect, send_file
from s3_helper import list_all_files, download, upload

app = Flask(__name__)

UPLOAD_FOLDER = "upload_files"
BUCKET = "prueba-15-sept-mlops"
#PREFIX = "1/1b44b22a64f84d95a31779c3d8fb3adb/artifacts/prophet_2"
@app.route('/')
def start():
    return "MODELO PROPHET LISTO PARA ACTUALIZAR Y EJECUTAR"

@app.route("/home")
def home():
    contents = list_all_files(bucket=BUCKET)
    return render_template('s3_storage_dashboard.html', contents=contents)

@app.route("/upload", methods=['POST'])
def upload_files():
    if request.method == "POST":
        f = request.files['file']
        f.save(os.path.join(UPLOAD_FOLDER, f.filename))
        upload(f"upload_files/{f.filename}", BUCKET, f.filename)
        return redirect("/home")


@app.route("/download/<filename>", methods=['GET'])
def download_files(filename):
    if request.method == 'GET':
        output = download(filename, BUCKET)
        return send_file(output, as_attachment=True)

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
