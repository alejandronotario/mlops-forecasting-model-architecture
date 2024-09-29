""" Dag para entrenar el modelo."""
import sys
sys.path.append("/opt/airflow/")
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from src.data.data import read_first_record_from_postgres, read_last_record_from_postgres, load_interval_data
from src.model.model_train import train, execute

logger = logging.getLogger(__name__)

HOST_NAME = 'postgres'
DATABASE = 'airflow'
USER_NAME = 'airflow'
PASSWORD = 'airflow'

dag = DAG(
    dag_id='dag_ejecucion',
    start_date=airflow.utils.dates.days_ago(0),
    schedule=None #'0 0 * * 0' para este caso un domingo a las 00:00
)

execute_model = PythonOperator(
    task_id="execute_model",
    python_callable=execute, dag=dag
)

execute_model