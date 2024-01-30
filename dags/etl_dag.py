from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
import configparser
import logging
import smtplib
import os

# Lectura de archivo de configuración
config = configparser.ConfigParser()
config.read('config.ini')

# Rutas de archivos CSV temporales
extract_csv_path = '/tmp/extracted_data.csv'
transform_csv_path = '/tmp/transformed_data.csv'

# Parámetros del DAG
default_args = {
    'owner': 'Lucas Andrini',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición de formato de mensajes en logs
logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

# Instancia del DAG
dag = DAG(
    'etl_weather_data',
    default_args=default_args,
    description='ETL for weather data',
    schedule_interval='0 0,6,12,18 * * *',
)

# Funciones ETL
def extract(**kwargs):
    api_key = config.get('openweathermap', 'api_key')
    cities = config.get('openweathermap', 'cities')  # Latitudes y longitudes de ciudades separadas por comas
    cities = [city.strip() for city in cities.split(',')]

    weather_data = []

    for city in cities:
        lat, lon = map(float, city.split(';'))

        url_base = "https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_key}&units={units}"
        parametros = {
            'lat': lat,
            'lon': lon,
            'API_key': api_key,
            'mode': 'json',
            'units': 'metric'
        }
        url = url_base.format(**parametros)
        response = requests.get(url)

        if response.status_code == 200:
            data_dict = response.json()
            weather_data.append(data_dict)
            logging.info(f"Extracción de datos para la ciudad ({lat}, {lon}) OK.")
        else:
            logging.info(f"Error en la solicitud a la API para la ciudad ({lat}, {lon}).")

    # Guardar todos los datos en el archivo CSV temporal de extract
    df = pd.DataFrame(weather_data)
    df.to_csv(extract_csv_path, index=False)

def transform(**kwargs):
    # Cargar datos desde el archivo CSV temporal de extract
    df_extracted = pd.read_csv(extract_csv_path)
    
    keys_columns = ['weather', 'main', 'visibility', 'wind', 'dt']
    data_selection = {}
    for key, value in df_extracted.to_dict(orient='records')[0].items():
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
    df_transformed = pd.DataFrame([data_selection])
    columnas_drop = ['id', 'icon', 'deg']
    df_transformed = df_transformed.drop(columnas_drop, axis=1)
    df_transformed['dt'] = pd.to_datetime(df_transformed['dt'], unit='s', utc=True)
    df_transformed['dt'] = df_transformed['dt'].dt.tz_convert('America/Buenos_Aires')
    
    # Guardar datos en el archivo CSV temporal de transform
    df_transformed.to_csv(transform_csv_path, index=False)
    logging.info(f"Transformación de datos ok.")

def load(**kwargs):
    # Cargar datos desde el archivo CSV temporal de transform
    df_transformed = pd.read_csv(transform_csv_path)

    user = config.get('redshift', 'user')
    password = config.get('redshift', 'password')
    host = config.get('redshift', 'host')
    port = config.get('redshift', 'port')
    database = config.get('redshift', 'database')
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
    df_transformed.to_sql(nombre_tabla_redshift, engine, if_exists='append', index=False)
    logging.info(f"Carga de datos en Redshift ok.")
    
def check_temperature(**kwargs):
    # Cargar datos desde el archivo CSV temporal de transform
    df_transformed = pd.read_csv(transform_csv_path)
    
    # Obtener los valores de temperatura de la última ejecución
    last_temperature = df_transformed['temperature'].iloc[-1]

    # Obtener los límites de temperatura desde el archivo de configuración
    min_temperature = config.getfloat('temperature_thresholds', 'min_temperature')
    max_temperature = config.getfloat('temperature_thresholds', 'max_temperature')

    # Verificar si se superan los límites
    if min_temperature <= last_temperature <= max_temperature:
        return 'send_email_task'

def send_mail(**kwargs):
    try:
        # Obtengo datos de contexto
        fecha_envio = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Obtengo límites de temperatura desde el archivo de configuración
        min_temperature = config.getfloat('temperature_thresholds', 'min_temperature')
        max_temperature = config.getfloat('temperature_thresholds', 'max_temperature')

        # Cargar datos desde el archivo CSV temporal de transform
        df_transformed = pd.read_csv(transform_csv_path)
        temperatura_detectada = df_transformed['temperature'].iloc[-1]

        # Determinar cuál límite se supero
        if temperatura_detectada < min_temperature:
            limite_superado = f'Límite inferior ({min_temperature}°C)'
        elif temperatura_detectada > max_temperature:
            limite_superado = f'Límite superior ({max_temperature}°C)'

        # Cuerpo del mensaje con datos de contexto y límites
        subject = 'Alerta de temperatura'
        body_text = 'Fecha de envío: {}\nTemperatura detectada: {}°C\nLímite superado: {}'.format(
            fecha_envio, temperatura_detectada, limite_superado)
        message = 'Subject: {}\n\n{}'.format(subject, body_text)

        # Obtengo datos de mail desde el archivo de configuración
        remitente = config.get('mail_data', 'remitente')
        password = config.get('mail_data', 'password')
        destinatario = config.get('mail_data', 'destinatario')
        smtp_config = config['smtp_config']
        smtp_server = smtp_config.get('smtp_server')
        smtp_port = smtp_config.getint('smtp_port')

        # Configuración envío de mail
        x = smtplib.SMTP(smtp_server, smtp_port)
        x.starttls()
        x.login(remitente, password)
        x.sendmail(remitente, destinatario, message)
        logging.info('Correo electrónico enviado con éxito.')

    except Exception as exception:
        logging.error(exception)
        logging.error('Error al enviar el correo electrónico.')

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

check_temperature_task = PythonOperator(
    task_id='check_temperature',
    python_callable=check_temperature,
    provide_context=True,
    dag=dag,
)

send_mail_task = PythonOperator(
    task_id='send_email_task',
    python_callable=send_mail,
    provide_context=True,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=lambda: [os.remove(file) for file in [extract_csv_path, transform_csv_path]],
    dag=dag,
)

# Dependencias de tareas
extract_task >> transform_task >> load_task
transform_task >> check_temperature_task >> send_mail_task
load_task >> cleanup_task
send_mail_task >> cleanup_task