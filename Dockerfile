FROM apache/airflow:2.8.0
COPY requirements.txt .
USER airflow
RUN pip install --no-cache-dir -r requirements.txt