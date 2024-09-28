""" Dag para cargar los datos incrementale y de ejcución de modelo."""
import sys
sys.path.append("/opt/airflow/")
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import logging
from src.data.data import load_incremental_data, get_data, read_last_record_from_postgres


logger = logging.getLogger(__name__)

HOST_NAME = 'postgres'
DATABASE = 'airflow'
USER_NAME = 'airflow'
PASSWORD = 'airflow'

dag = DAG(
    dag_id='dag_carga_incremental',
    start_date=airflow.utils.dates.days_ago(0),
    schedule=None #'0 0 * * 0' para este caso un domingo a las 00:00
)

# read_data_from_postgres = PythonOperator(
#     task_id="read_data_from_postgres",
#     op_kwargs={
#         'fecha_foto': '2020-01-01'
#     },
#     python_callable=read_data_from_postgres,
#     dag=dag
# )

read_last_record = PythonOperator(
    task_id="read_last_record",
    python_callable=read_last_record_from_postgres,
    dag=dag
)

get_gas_data = PythonOperator(
        task_id="get_gas_data", python_callable=get_data, dag=dag
    )

load_incremental_gas_data = PythonOperator( 
        task_id="load_incremental_gas_data",
        op_kwargs={
        'db_host': HOST_NAME,
        'db_name': DATABASE,
        'db_user': USER_NAME,
        'db_pswd': PASSWORD,
        'key': 'gas_data',
        'task_id': 'get_gas_data',
        'key_2': 'last_record',
        'task_id_2': 'read_last_record',
        'days': 7
    }, python_callable=load_incremental_data, dag=dag
)
read_last_record >> get_gas_data >> load_incremental_gas_data