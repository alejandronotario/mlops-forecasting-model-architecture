import json
import requests
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def get_data(ti):
    """
    Respuesta de la API
    """
    url = 'https://www.gasnetworks.ie/corporate/open-data/Daily-Supply-Q2-2024.json'
    res = requests.get(url).json()
    logger.info(res)
    ti.xcom_push(key="gas_data", value=res)

def load_data(ti, db_host, db_name, db_user, db_pswd):
    """
    Carga en Postgres los datos de entrada
    """
    gas_data = ti.xcom_pull(key="gas_data", task_ids="get_gas_data")
    logger.info(gas_data)
    records_list = []

    for r in gas_data:
        rows = {
            'ds': r['Date'],
            'y': r['Corrib production']
        }
        records_list.append(rows)
    df = pd.DataFrame(records_list)
    logger.info("CONTEO: ", df.count())
    """
    En la configuración del motor es imprescindible que los argumentos sean:
    db_user: usuario incluido en docker compose
    df_pswd: pasword incluido en docker compose
    db_host: nombre del servicio sobre el que está la imagen postgres
    db_name: nombre de la base de datos en docker compose
    """
   
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pswd}@{db_host}/{db_name}")
    df.to_sql('gas_supply', con=engine, schema='public', if_exists='replace', index=False)

    logger.info(f"Exito: Cargados {len(df)} registros en {db_name}.")

def read_data_from_postgres():
    """
    Lectura de Postgres los datos de entrada
    """
    registros = []
    request = "SELECT * FROM public.gas_supply"
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    for source in sources:
        registros.append(source)
    logger.info(source)
    return registros


    
def read_last_record_from_postgres():
    """
    Lectura de último registro de Postgres        
    """
    hook = PostgresHook(postgres_conn_id="postgres")
    df = hook.get_pandas_df(sql="SELECT * FROM public.gas_supply ORDER BY dat DESC LIMIT 1")
    logger.info(df.head())