FROM apache/airflow:2.8.0
COPY requirements.txt .
RUN pip install -r requirements.txt