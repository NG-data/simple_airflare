FROM apache/airflow:2.9.2


USER airflow


COPY requirements.txt /requirements.txt


USER airflow
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt