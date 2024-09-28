from airflow import DAG
import airflow
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import json
import requests
from datetime import datetime
import time
import psycopg2
from sqlalchemy import create_engine
import logging
import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation
from prophet.diagnostics import performance_metrics
import itertools
import numpy as np
import mlflow
from mlflow.models import infer_signature


param_grid = {  
    'changepoint_prior_scale': [0.001, 0.01, 0.1, 0.5],
    'seasonality_prior_scale': [0.01, 0.1, 1.0, 10.0],
    'holidays_prior_scale': [0.01, 0.1, 1.0, 10.0],
    'seasonality_mode': ['additive', 'multiplicative']
    }




def train(ti):
    gas_data = ti.xcom_pull(key="gas_data", task_ids="load_data")
    df = pd.DataFrame(json.loads(gas_data))
    df['ds'] = pd.to_datetime(df['ds'], format='%d/%m/%Y')
    initial = int(0.7*(df.shape[0]))
    horizon = initial / 3
    period = horizon / 4 
    input_example = df[:int(0.1*(df.shape[0]))]
    mlflow.set_experiment("time series")

    with mlflow.start_run() as run:
        mlflow.set_experiment("time series")
        run_id = run.info.run_id
        logger.info(f"\nActive run_id: {run_id}")
        all_params = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]
        rmses = []
        for params in all_params:
            m = Prophet(**params).fit(df)  # Fit model with given params
            df_cv = cross_validation(m, initial='{} days'.format(initial),
                period='{} days'.format(period), horizon = '{} days'.format(horizon))
            logger.info(df_cv)
            df_p = performance_metrics(df_cv, rolling_window=1)
            logger.info(df_p)
            rmses.append(df_p['rmse'].values[0])
        logger.info(rmses)
        rmse_min = int(min(rmses))
        logger.info(rmse_min)
        #metrics_dict = {'rmse': rmse_min}
        mlflow.log_metric("rmse", rmse_min)
        tuning_results = pd.DataFrame(all_params)
        tuning_results['rmse'] = rmses
        best_params = {}
        best_params_list = list(all_params[np.argmin(rmses)].items())
        best_params[best_params_list[0][0]] = best_params_list[0][1]
        best_params[best_params_list[1][0]] = best_params_list[1][1]
        best_params[best_params_list[2][0]] = best_params_list[2][1]
        best_params[best_params_list[3][0]] = best_params_list[3][1]
        logger.info(best_params)
        mlflow.log_params(best_params)
        model = Prophet(**best_params).fit(df)
        train = model.history
        predictions = model.predict(model.make_future_dataframe(30))
        signature = infer_signature(train, predictions)
        #mlflow.log_params(m.get_params())
        mlflow.prophet.log_model(pr_model=model, artifact_path="prophet",
                                 input_example=input_example,
                                 registered_model_name="prophet",
                                 signature=signature)
        model_uri = mlflow.get_artifact_uri("prophet")
    return run_id, model_uri