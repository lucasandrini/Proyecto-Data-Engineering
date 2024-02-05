# ETL Datos Meteorológicos

## Descripción General
Este DAG de Airflow está diseñado para realizar un proceso ETL de datos meteorológicos. Los datos se extraen de la API de OpenWeatherMap, se transforman, y se cargan en una base de datos AWS Redshift. Se envían alertas por correo electrónico en caso de temperaturas que sobrepasen los limites establecidos.

## Configuración
Editar el archivo de configuración `config.ini` ubicado en `/opt/airflow/dags/` con los parámetros necesarios. A continuación se detallan las secciones y claves requeridas:

### Configuración de la API de OpenWeatherMap
```ini
[openweathermap]
api_key = TU_CLAVE_DE_API_DE_OPENWEATHERMAP
cities = LATITUD1;LONGITUD1, LATITUD2;LONGITUD2, ...

[redshift]
user = TU_USUARIO_DE_REDSHIFT
password = TU_CONTRASEÑA_DE_REDSHIFT
host = TU_HOST_DE_REDSHIFT
port = TU_PUERTO_DE_REDSHIFT
database = TU_BASE_DE_DATOS_DE_REDSHIFT

[temperature_thresholds]
min_temperature = UMBRAL_DE_TEMPERATURA_MÍNIMO
max_temperature = UMBRAL_DE_TEMPERATURA_MÁXIMO

[mail_data]
remitente = TU_DIRECCIÓN_DE_CORREO_ELECTRÓNICO
password = TU_CONTRASEÑA_DE_CORREO_ELECTRÓNICO
destinatario = DIRECCIÓN_DE_CORREO_ELECTRÓNICO_DEL_DESTINATARIO

[smtp_config]
smtp_server = TU_SERVIDOR_SMTP
smtp_port = TU_PUERTO_SMTP
```

## Estructura del DAG
Este DAG se llama etl_weather_data y consta de las siguientes tareas:

### Tarea de Extracción (extract):
* Obtiene datos meteorológicos de la API de OpenWeatherMap para las ciudades especificadas.
* Guarda los datos en un archivo CSV temporal.

### Tarea de Transformación (transform):
* Lee el archivo CSV extraído.
* Transforma los datos, descartando los irrelevantes y ajustando los tipos.
* Guarda los datos transformados en otro archivo CSV temporal.

### Tarea de Carga (load):
* Lee el archivo CSV transformado.
* Carga los datos en una tabla de AWS Redshift llamada weather_cities.

### Tarea de Envío de Correo Electrónico (send_email_task):
* Verifica si temperatura sobrepase el nivel minimo o maximo establecido en el CSV de datos transformados.
* Envía alertas por correo electrónico si se encuentran anomalías.
* El mail enviado tiene la siguiente estructura:
* 	Fecha_de_envío
* 	| Country | Name    | Temp    | Limite  |
	| ------- | ------- | ------- | ------- |
	| Val 1,1 | Val 1,2 | Val 1,3 | Val 1,4 |
	| Val 2,1 | Val 2,2 | Val 2,3 | Val 2,4 | 

### Tarea de Limpieza (cleanup_temp_files):
* Elimina archivos CSV temporales creados durante el proceso ETL.

## Parámetros del DAG
* Fecha de Inicio: Fecha y hora actuales
* Reintentos: 1
* Retardo en Reintentos: 1 minuto

## Programación
El DAG está programado para ejecutarse cada 6 horas a las 0, 6, 12 y 18 UTC.

## Registro
Los logs están configurados para mostrar marcas de tiempo, niveles y mensajes para facilitar el monitoreo en Apache Airflow.

## Notas
* Se debe asegurar que Airflow tenga las dependencias necesarias instaladas, es decir, las bibliotecas que se encuentran en `requeriments.txt`.
* La base de datos `weather_cities` ya debe existir antes de la ejecución del DAG. Dentro del repositorio se encuentra la query para su creación:
```
CREATE TABLE weather_cities (
	lon decimal(10,2),
	lat decimal(10,2),
    main char(20),
    description char(40),
    temp decimal(10,2),
    feels_like decimal(10,2),
    temp_min decimal(10,2),
    temp_max decimal(10,2),
    pressure decimal(10,2),
    humidity decimal(10,2),
    visibility integer,
    speed decimal(10,2),
    dt timestamp,
    country char(4),
    name char(40),
    PRIMARY KEY (name, dt)
);
```
* Para el envío del mail de alerta, en caso de usar gmail, se debe configurar una contraseña de aplicación. Para más información consultar el siguiente hilo: [Stack Overflow](https://stackoverflow.com/questions/59188483/error-invalid-login-535-5-7-8-username-and-password-not-accepted)
* El archivo `cities_list.xlsx` contiene ciudades con la información de longitud y latitud, las cuales se pueden agregar en `config.ini` para obtener la informacion del clima.
