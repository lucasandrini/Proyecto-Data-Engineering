import requests
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
import psycopg2
from sqlalchemy.engine.url import URL

def runQuery(sql):
    result = engine.connect().execute((text(sql)))
    return pd.DataFrame(result.fetchall(), columns=result.keys())

# URL base 
url_base = "https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_key}&units={units}"

# Valores de los parámetros
parametros = {
    'lat': -34.61,
    'lon': -58.38,
    'API_key': '654c7b47cda32dc0a894e1a875c3b9a2',  
    'mode': 'json',  
    'units': 'metric'
}

# URL con parametros
url = url_base.format(**parametros)

# Solicitud GET
response = requests.get(url)

# Verifico si la solicitud fue exitosa (código de estado 200)
if response.status_code == 200:
    # Respuesta en diccionario de Python
    data_dict = response.json() 
else:
    print(f"Error en la solicitud. Código de estado: {response.status_code}")

# Claves del diccionario a seleccionar
keys_columns = ['weather', 'main', 'visibility', 'wind', 'dt']

# Diccionario de seleccion de datos
data_selection = {}

# Recorro el diccionario con la info de la API
for key, value in data_dict.items():
    if key in keys_columns:
        # Si el valor dentro del diccionario es otro diccionario:
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                data_selection[sub_key] = sub_value
        # Si el valor dentro del diccionario es una lista de diccionarios:     
        elif isinstance(value, list):  
            for item in value:  
                if isinstance(item, dict):  
                    for nested_key, nested_value in item.items():  
                        data_selection[nested_key] = nested_value
        # Si el valor es un par clave-valor:
        else:
            data_selection[key] = value

# Creo el DataFrame
df = pd.DataFrame([data_selection])

# Transformaciones
columnas_drop = ['id', 'icon', 'deg']
df_final = df.drop(columnas_drop, axis=1)
df_final['dt'] = pd.to_datetime(df_final['dt'], unit='s', utc=True)
df_final['dt'] = df_final['dt'].dt.tz_convert('America/Buenos_Aires')

# DataFrame resultante
print(df_final.to_string())

# Info de conexion
user = 'lucas_andrini_coderhouse'
password = 'CIpG6k50To'
host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
port = '5439'
database = 'data-engineer-database'

""" # String de conexión
connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'

# Objeto Engine de SQLAlchemy
engine = create_engine(connection_string) """

url = URL.create(
drivername='redshift+redshift_connector', # indicate redshift_connector driver and dialect will be used
host=host, # Amazon Redshift host
port=port, # Amazon Redshift port
database=database, # Amazon Redshift database
username=user, # Amazon Redshift username
password=password # Amazon Redshift password
)

engine = sqlalchemy.create_engine(url)

# Nombre de la tabla en Redshift
nombre_tabla_redshift = 'bsas'

# DataFrame a Redshift
df_final.to_sql(nombre_tabla_redshift, engine, if_exists='append', index=False)

print(f'DataFrame cargado exitosamente en la tabla {nombre_tabla_redshift} de Redshift.')