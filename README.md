# ETL Datos Meteorológicos

## Descripción General
Este DAG de Airflow está diseñado para realizar un proceso ETL de datos meteorológicos. Los datos se extraen de la API de OpenWeatherMap, se transforman, y se cargan en una base de datos AWS Redshift. Se envían alertas por correo electrónico en caso de temperaturas que sobrepasen los limites establecidos.

## Configuración
Editar el archivo de configuración `config.ini` ubicado en `/opt/airflow/` y `.env` con los parámetros necesarios. A continuación se detallan las secciones y claves requeridas:

### Archivo config.ini
```
[openweathermap]
cities = latitud1;longitud1, latitud2;longitud2, ...

[temperature_thresholds]
min_temperature = valor
max_temperature = valor

[smtp_config]
smtp_server = valor
smtp_port = valor
```

### Archivo .env
```
AIRFLOW_UID=50000

OPENWEATHERMAP_API_KEY=valor

MAIL_REMITENTE=valor
MAIL_PASSWORD=valor
MAIL_DESTINATARIO=valor

REDSHIFT_USER=valor
REDSHIFT_PASSWORD=valor
REDSHIFT_HOST=valor
REDSHIFT_PORT=valor
REDSHIFT_DATABASE=valor
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
  	- Fecha_de_envío
	- | Country | Name    | Temp    | Limite  |
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

## Funcionamiento del ETL en imagenes
### Inicio Airflow
![](https://github.com/lucasandrini/Proyecto-Data-Engineering/blob/main/Imagenes/dag%20airflow%20inicio.png)

### Airflow Graph
![](https://github.com/lucasandrini/Proyecto-Data-Engineering/blob/main/Imagenes/airflow%20graph.png)

### Ejecucion
![](https://github.com/lucasandrini/Proyecto-Data-Engineering/blob/main/Imagenes/dag%20tarea%20ejecutada.png)

### Log Extract
![](https://github.com/lucasandrini/Proyecto-Data-Engineering/blob/main/Imagenes/log%20extract.png)

### Log Transform
![](https://github.com/lucasandrini/Proyecto-Data-Engineering/blob/main/Imagenes/log%20transform.png)

### Log Load
![](https://github.com/lucasandrini/Proyecto-Data-Engineering/blob/main/Imagenes/log%20load.png)

### Log Send Mail
![](https://github.com/lucasandrini/Proyecto-Data-Engineering/blob/main/Imagenes/log%20mail.png)

### Log Clean
![](https://github.com/lucasandrini/Proyecto-Data-Engineering/blob/main/Imagenes/log%20clean.png)

### Base de datos Redshift (DBeaver)
![](https://github.com/lucasandrini/Proyecto-Data-Engineering/blob/main/Imagenes/imagen%20db.png)

### Alerta por mail
![](https://github.com/lucasandrini/Proyecto-Data-Engineering/blob/main/Imagenes/imagen%20mail%20alerta.png)
