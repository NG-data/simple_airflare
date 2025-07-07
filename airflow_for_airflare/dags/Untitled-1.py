# %%
import requests
import pandas as pd
# Устанавливает соединение с postgres
from airflow.providers.postgres.hooks.postgres import PostgresHook
# Устанавливает соединение с S3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG, models
import datetime as dt
from airflow.operators.python import PythonOperator
import logging  # Логирование
import json  # Сериализация и десириализация json в строку для загрузки в S3
# Говорит «Это не путь, а строка — обращайся с ней как с файлоподобным объектом».
from io import StringIO

# %%


# Функция загружает данные в S3, data-файл для загрузки, bucket - имя бакета, key - путь и название файла
def upload_to_s3(data, bucket='airflare', key='temp/response.json'):
    logging.info('Создание подключения к S3')
    hook = S3Hook(aws_conn_id='minio')  # Установка подключения к S3
    # Сериализует JSON в строку для передачи в S3(так как в S3 нельзя передавать объекты python)
    json_data = data

    logging.info('Подключение установлено')

    # Проверяет есть ли бакет с таким именем
    if not hook.check_for_bucket(bucket):
        hook.create_bucket(bucket_name=bucket)

    logging.info('Загрузка данных в S3')

    hook.load_string(  # Загружает строку в S3
        string_data=json_data,  # Сериализованные в строку данные
        key=key,  # Путь до файла в заданном бакете
        bucket_name=bucket,  # Название бакета
        replace=True
    )

    logging.info('Загрузка завершена успешно')

    # Возвращает путь до файла и название бакета
    return {'bucket': bucket, 'key': key}

# %%


# Функция выгружает сериальизованный в строку файл из S3
def load_from_s3(bucket, key, aws_conn_id='minio'):
    logging.info('Создание подключения к S3')
    # Задает переменную подключения где aws_conn_id это id подключения к S3 в UI airflow
    conn = S3Hook(aws_conn_id=aws_conn_id)

    logging.info('Подключение установлено, загрузка данных из S3')

    # Читает сериализованный файл в заданном бакете по заданному пути
    content = conn.read_key(bucket_name=bucket, key=key)

    data = json.loads(content)  # Десериализует файл
    logging.info('Данные выгружены из S3')
    return data

# %%


def extract(**context):
    headers = {'x-access-token': '167317b476e1808633c659a8bbb35b13'}

    url = "https://api.travelpayouts.com/v2/prices/latest"

    querystring = {"currency": "rub",
                   "origin": "IKT",
                   "destination": "HKT",
                   'beginning_of_period': '2025-09-01',
                   "period_type": "year",
                   'one_way': True,
                   "page": "1",
                   "limit": "1000",
                   "show_to_affiliates": "true",
                   "sorting": "price",
                   "trip_class": "0"}

    logging.info('Начало извлечения данных')

    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code != 200:  # Проверка на отклик API
        logging.error('Ошибка при чтении данных из API')
        raise ValueError('Ошибка')

    # Полученный ответ оборачивает в JSON затем сериализуется в строку и загружается по заданному пути в S3
    path = upload_to_s3(json.dumps(response.json()), key='temp/response.json')

    logging.info('Данные успешно сохранены во временное хранилище')

    # При помощи Xcom мы отправляем в другой DAG данные о местоположении данных по API
    context['ti'].xcom_push(key='df_dirty', value=path)

    logging.info('Данные успешно извлечены и переданы в Xcom')

# %%


def transform(**context):  # Функция оставляет только нужные нам данные
    # Выгружает из Xcom путь к файлу по ключу key='df_dirty'
    path = context['ti'].xcom_pull(key='df_dirty')

    logging.info('Данные приняты из Xcom, начат процесс очистки')

    # Выгружает файл из S3 и десериализует его
    file = load_from_s3(bucket=path['bucket'], key=path['key'])

    df = pd.DataFrame(file['data'])
    df = df[['depart_date', 'origin', 'destination', 'trip_class',
             'value', 'gate', 'duration', 'distance', 'number_of_changes']]

    logging.info('Преобразование завершено, начинается передача DF в S3')

    # Преобразует DF в JSON
    content = df.to_json(index=False, orient='records')

    path = upload_to_s3(
        content, bucket=path['bucket'], key='temp/df.json')  # Загружает в S3

    logging.info('Данные успешно сохранены в S3, передаю их в Xcom')

    # Отправляет путь до сохраненного Файла
    context['ti'].xcom_push(key='path', value=path)

    logging.info('данные успешно очищены и записаны в хранилище')

# %%


def load(**context):  # Функция загружает данные в БД
    # Выгружает путь до файла в бакете из Xcom
    path = context['ti'].xcom_pull(key='path')
    logging.info(
        'Данные из Xcom успешно выгружены, начинаю выгружать данные из S3')

    # Выгружает из S3 файл который внутри содержит список словарей
    content = load_from_s3(bucket=path['bucket'], key=path['key'])
    df = pd.DataFrame(content)
    logging.info('Данные получены из хранилища, начат процесс отправки в БД')

    # Указывать имя при соединении с БД которое указано в yaml
    hook = PostgresHook(postgres_conn_id='postgres_airfare')
    engine = hook.get_sqlalchemy_engine()

    # Выводит в логи размерность DF
    logging.info(f'Размер DataFrame: {df.shape}')
    # Выводит в логи типы данных каждого столбца DF
    logging.info(f'Типы данных:\n{df.dtypes}')

    logging.info('Связь с БД установлена')
    try:
        df.to_sql('irk_hkt_year_stats', engine, if_exists='append',
                  index=False)  # Отправляет данные в заданную таблицу
    # Логирует любую ошибку(исключение) и записывает ее в переменную
    except Exception as e:
        logging.error(f'Ошибка при загрузки в БД {e}')
    logging.info('процесс ETL успешно завершен, данные загружены в БД')


# %%
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=1)
}

with DAG(dag_id='ETL_airflare_data', default_args=default_args, schedule_interval='0 12 * * *', catchup=False, tags=['cost']) as dag:
    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load
    )

    task_extract >> task_transform >> task_load
