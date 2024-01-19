# etl_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from airflow.hooks.base_hook import BaseHook
from sqlalchemy.engine.url import URL

# Parámetros del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instancia del DAG
dag = DAG(
    'etl_weather_data',
    default_args=default_args,
    description='ETL job for weather data',
    schedule_interval='@daily',
)

# Funciones ETL
def extract(**kwargs):
    url_base = "https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_key}&units={units}"
    parametros = {
        'lat': -34.61,
        'lon': -58.38,
        'API_key': '654c7b47cda32dc0a894e1a875c3b9a2',
        'mode': 'json',
        'units': 'metric'
    }
    url = url_base.format(**parametros)
    response = requests.get(url)
    if response.status_code == 200:
        data_dict = response.json()
    else:
        print(f"Error en la solicitud. Código de estado: {response.status_code}")
    return data_dict

def transform(**kwargs):
    ti = kwargs['ti']
    data_dict = ti.xcom_pull(task_ids='extract')
    keys_columns = ['weather', 'main', 'visibility', 'wind', 'dt']
    data_selection = {}
    for key, value in data_dict.items():
        if key in keys_columns:
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    data_selection[sub_key] = sub_value
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        for nested_key, nested_value in item.items():
                            data_selection[nested_key] = nested_value
            else:
                data_selection[key] = value
    df = pd.DataFrame([data_selection])
    columnas_drop = ['id', 'icon', 'deg']
    df_final = df.drop(columnas_drop, axis=1)
    df_final['dt'] = pd.to_datetime(df_final['dt'], unit='s', utc=True)
    df_final['dt'] = df_final['dt'].dt.tz_convert('America/Buenos_Aires')
    return df_final

def load(**kwargs):
    ti = kwargs['ti']
    df_final = ti.xcom_pull(task_ids='transform')
    user = 'lucas_andrini_coderhouse'
    password = 'CIpG6k50To'
    host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
    port = '5439'
    database = 'data-engineer-database'
    url = URL.create(
        drivername='redshift+redshift_connector',
        host=host,
        port=port,
        database=database,
        username=user,
        password=password
    )
    engine = sqlalchemy.create_engine(url)
    nombre_tabla_redshift = 'bsas'
    df_final.to_sql(nombre_tabla_redshift, engine, if_exists='append', index=False)
    print(f'DataFrame cargado exitosamente en la tabla {nombre_tabla_redshift} de Redshift.')

# Tareas del DAG
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

# Definir dependencias
extract_task >> transform_task >> load_task