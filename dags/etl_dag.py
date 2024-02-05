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
import ast
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Ruta absoluta del archivo de configuración
ruta_del_archivo_config = '/opt/airflow/dags/config.ini'

# Lectura de archivo de configuración
config = configparser.ConfigParser()
config.read(ruta_del_archivo_config)

# Rutas de archivos CSV temporales
extract_csv_path = '/tmp/extracted_data.csv'
transform_csv_path = '/tmp/transformed_data.csv'

# Parámetros del DAG
default_args = {
    'owner': 'Lucas Andrini',
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
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

# Funciones
def extract(**kwargs):

    # Datos API
    api_key = config.get('openweathermap', 'api_key')
    cities = config.get('openweathermap', 'cities')  # Latitudes y longitudes de ciudades separadas por comas
    cities = [city.strip() for city in cities.split(',')]

    weather_data = []

    # Recorrer listado de ciudades y obtener datos de la API
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

    # Guardar todos los datos en archivo CSV temporal de extract
    df = pd.DataFrame(weather_data)
    df.to_csv(extract_csv_path, index=False)

    # Log contenido del DataFrame después de la extracción
    logging.info(f"Contenido del DataFrame después de la extracción:\n{df}")

def transform(**kwargs):
    # Cargar datos desde el archivo CSV temporal de extract
    df_extracted = pd.read_csv(extract_csv_path, converters={'coord': eval, 'weather': ast.literal_eval, 'main': eval, 'wind': eval, 'sys': eval})

    # Lista para almacenar diccionarios de filas transformadas
    rows_transformed = []

    # Recorrer las filas del DataFrame df_extracted
    for indice, fila in df_extracted.iterrows():
        # Crear un diccionario para almacenar los valores de esta fila
        fila_transformed = {}

        # Extraer datos de la columna 'coord'
        fila_transformed['lon'] = fila['coord']['lon']
        fila_transformed['lat'] = fila['coord']['lat']

        # Extraer datos de la columna 'weather'
        if fila['weather']:
            # Tomar el primer elemento de la lista 'weather'
            primer_weather = fila['weather'][0]
            fila_transformed['main'] = primer_weather.get('main', '')
            fila_transformed['description'] = primer_weather.get('description', '')

        # Extraer datos de la columna 'main'
        fila_transformed['temp'] = fila['main']['temp']
        fila_transformed['feels_like'] = fila['main']['feels_like']
        fila_transformed['temp_min'] = fila['main']['temp_min']
        fila_transformed['temp_max'] = fila['main']['temp_max']
        fila_transformed['pressure'] = fila['main']['pressure']
        fila_transformed['humidity'] = fila['main']['humidity']

        # Extraer datos de la columna 'visibility'
        fila_transformed['visibility'] = fila['visibility']

        # Extraer datos de la columna 'wind'
        fila_transformed['speed'] = fila['wind']['speed']

        # Extraer datos de la columna 'dt'
        fila_transformed['dt'] = fila['dt']

        # Extraer datos de la columna 'sys'
        fila_transformed['country'] = fila['sys']['country']

        # Extraer datos de la columna 'name'
        fila_transformed['name'] = fila['name']

        # Agregar el diccionario a la lista
        rows_transformed.append(fila_transformed)

    # Convertir la lista de diccionarios en un DataFrame
    df_transformed = pd.DataFrame(rows_transformed)

    df_transformed['dt'] = pd.to_datetime(df_transformed['dt'], unit='s', utc=True)

    # Log contenido del DataFrame después de la transformacion
    logging.info(f"Contenido de df_transformed:\n{df_transformed.to_string()}")

    # Guardar datos en el archivo CSV temporal de transform
    df_transformed.to_csv(transform_csv_path, index=False)
    logging.info(f"Transformación de datos ok.")

def load(**kwargs):
    # Cargar datos desde el archivo CSV temporal de transform
    df_transformed = pd.read_csv(transform_csv_path)

    # Log contenido del DataFrame después de la transformacion
    logging.info(f"Contenido de df_transformed:\n{df_transformed.to_string()}")

    # Configuracion de conexion a AWS Redshift
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
    nombre_tabla_redshift = 'weather_cities'
    df_transformed.to_sql(nombre_tabla_redshift, engine, if_exists='append', index=False)
    logging.info(f"Carga de datos en Redshift ok.")
    
def send_mail(**kwargs):
    try:
        # Cargar datos desde el archivo CSV temporal de transform
        df_transformed = pd.read_csv(transform_csv_path)

        logging.info(f"Tipo de datos temp: {df_transformed['temp'].dtype}")

        # Obtener los límites de temperatura desde el archivo de configuración
        min_temperature = config.getfloat('temperature_thresholds', 'min_temperature')
        logging.info(f"min temp: {min_temperature}")

        max_temperature = config.getfloat('temperature_thresholds', 'max_temperature')
        logging.info(f"max temp: {max_temperature}")

        # Verificar si se superan los límites para cada fila
        rows_to_notify = df_transformed[
            (df_transformed['temp'] < min_temperature) | (df_transformed['temp'] > max_temperature)
        ]

        # Agregar columna 'limite' con la información del límite roto
        rows_to_notify.loc[df_transformed['temp'] < min_temperature, 'limite'] = min_temperature
        rows_to_notify.loc[df_transformed['temp'] > max_temperature, 'limite'] = max_temperature

        # Seleccionar solo las columnas country, name, temp y limite
        rows_to_notify = rows_to_notify[['country', 'name', 'temp', 'limite']]

        logging.info("Filas a notificar:")
        logging.info(rows_to_notify)

        if not rows_to_notify.empty:
            # Cuerpo del mensaje con datos de contexto y límites
            fecha_envio = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            subject = 'Alerta de temperatura'

            # Convertir el DataFrame a una tabla HTML
            html_table = rows_to_notify.to_html(index=False)

            # Crear objeto MIMEMultipart
            message = MIMEMultipart()
            message['Subject'] = subject

            # Adjuntar el HTML al cuerpo del mensaje
            body_text = f"Fecha de envío: {fecha_envio}\n\n{html_table}"
            message.attach(MIMEText(body_text, 'html'))

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

            # Enviar el mensaje
            x.sendmail(remitente, destinatario, message.as_string())
            logging.info('Correo electrónico enviado con éxito.')

    except Exception as exception:
        logging.error(exception)
        logging.error('Error al enviar el correo electrónico.')

def cleanup_temp_files():
    # Archivos a ser eliminados
    files_to_remove = [extract_csv_path, transform_csv_path]

    # Verificar la existencia de archivos antes de la eliminación
    for file in files_to_remove:
        if os.path.exists(file):
            logging.info(f"Archivo '{file}' existe antes de la eliminación.")
        else:
            logging.info(f"Archivo '{file}' no existe antes de la eliminación.")

    # Eliminar archivos
    [os.remove(file) for file in files_to_remove]

    # Verificar la existencia de archivos después de la eliminación
    for file in files_to_remove:
        if os.path.exists(file):
            logging.error(f"Archivo '{file}' todavía existe después de la eliminación. ¡La tarea de limpieza puede no haber tenido éxito!")
        else:
            logging.info(f"Archivo '{file}' eliminado con éxito.")

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

send_mail_task = PythonOperator(
    task_id='send_email_task',
    python_callable=send_mail,
    provide_context=True,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    dag=dag,
)

# Dependencias de tareas
extract_task >> transform_task >> load_task
transform_task >> send_mail_task
load_task >> cleanup_task
send_mail_task >> cleanup_task