# %%
import requests
import pandas as pd
# Устанавливает соединение с postgres
from airflow.providers.postgres.hooks.postgres import PostgresHook
# Устанавливает соединение с S3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG, models
from airflow.decorators import dag, task
import datetime as dt
from airflow.operators.python import PythonOperator
import logging  # Логирование
import json  # Сериализация и десириализация json в строку для загрузки в S3
# Говорит «Это не путь, а строка — обращайся с ней как с файлоподобным объектом».
from io import StringIO

# %%
IATA_translate = {
    'IKT': 'Иркутск',
    'HKT': 'Пхукет',
    'MOW': 'Москва',
    'AER': 'Сочи',
    'LED': 'Санкт_Петербург',
    'VVO': 'Владивосток',
    'MLE': 'Мале',
    'DXB': 'Дубай',
    'IST': 'Стамбул',
    'AYT': 'Анталья',
    'NHA': 'Нячанг',
    'SGN': 'Хошимин',
    'HAN': 'Ханой',
    'TYO': 'Токио',
    'KUL': 'Куала_Лумпур',
    'PKX': 'Пекин_Дасин',
    'PEK': 'Пекин_Шоуду'
}


# %%
city_list = ['HKT', 'MOW', 'AER', 'LED', 'VVO', 'MLE', 'DXB', 'IST', 'AYT',
             'NHA', 'SGN', 'HAN', 'TYO', 'KUL', 'PKX', 'PEK']  # Города назначения
origin_city = 'IKT'  # Город вылета
routs = []

for i in city_list:  # Генерирует комбинацию билетов туда-обратно
    city_dickt = {}
    city_dickt['origin'] = origin_city
    city_dickt['destination'] = i
    city_dickt['bd'] = f'{IATA_translate[origin_city]}_{IATA_translate[i]}_outbound'
    routs.append(city_dickt)

    city_dickt = {}
    city_dickt['origin'] = i
    city_dickt['destination'] = origin_city
    city_dickt['bd'] = f'{IATA_translate[i]}_{IATA_translate[origin_city]}_return'
    routs.append(city_dickt)


# %%
# Функция загружает данные в S3, data-файл для загрузки, bucket - имя бакета, key - путь и название файла
def upload_to_s3(data, key, bucket='airflare'):
    logging.info('Создание подключения к S3')
    hook = S3Hook(aws_conn_id='minio')  # Установка подключения к S3

    logging.info('Подключение установлено')

    # Проверяет есть ли бакет с таким именем
    if not hook.check_for_bucket(bucket):
        hook.create_bucket(bucket_name=bucket)

    logging.info('Загрузка данных в S3')

    key = f"temp/{key}"

    hook.load_string(  # Загружает строку в S3
        string_data=data,  # Сериализованные в строку данные
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
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=1)
}

with DAG(dag_id='ETL_airflare_data', default_args=default_args, schedule_interval='0 12 * * *', catchup=False, tags=['cost']) as dag:

    # %%
    @task
    def extract(rout):
        origin = rout['origin']  # Место вылета
        destination = rout['destination']  # Горот назначения
        headers = {'x-access-token': '167317b476e1808633c659a8bbb35b13'}

        url = "https://api.travelpayouts.com/v2/prices/latest"

        querystring = {"currency": "rub",
                       "origin": f"{origin}",
                       "destination": f"{destination}",
                       'beginning_of_period': '2025-09-01',
                       "period_type": "year",
                       'one_way': True,
                       "page": "1",
                       "limit": "1000",
                       "show_to_affiliates": "true",
                       "sorting": "price",
                       "trip_class": "0"}

        logging.info(
            f'Начало извлечения данных для направления {origin}-{destination}')

        response = requests.get(url, headers=headers, params=querystring)

        if response.status_code != 200:  # Проверка на отклик API
            logging.error('Ошибка при чтении данных из API')
            raise ValueError('Ошибка')

        # Полученный ответ оборачивает в JSON затем сериализуется в строку и загружается по заданному пути в S3
        path = upload_to_s3(data=json.dumps(
            response.json()), key=f"response_{rout['origin']}_{rout['destination']}.json")

        logging.info('Данные успешно сохранены во временное хранилище')
        logging.info('Данные успешно извлечены и переданы в Xcom')

        return {'path': path, 'rout': rout}

# %%
    @task
    def transform(info):  # Функция оставляет только нужные нам данные
        path = info['path']
        rout = info['rout']
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
            content, bucket=path['bucket'], key=f"df_{rout['origin']}_{rout['destination']}.json")  # Загружает в S3

        logging.info('Данные успешно сохранены в S3, передаю их в Xcom')

        logging.info('данные успешно очищены и записаны в хранилище')
        return {'path': path, 'rout': rout}

# %%
    @task
    def load(info):  # Функция загружает данные в БД

        logging.info(
            'Данные из Xcom успешно выгружены, начинаю выгружать данные из S3')

        path = info['path']  # Путь до файла в S3
        rout = info['rout']  # Текущее направление

        # Выгружает из S3 файл который внутри содержит список словарей
        content = load_from_s3(bucket=path['bucket'], key=path['key'])

        df = pd.DataFrame(list(content))
        logging.info(
            'Данные получены из хранилища, начат процесс отправки в БД')

        # Указывать имя в airflow UI при соединении с БД которое указано в yaml(postgres_conn_id= это id в aiflow UI)
        hook = PostgresHook(postgres_conn_id='postgres_airfare')
        engine = hook.get_sqlalchemy_engine()

        # Выводит в логи размерность DF
        logging.info(f'Размер DataFrame: {df.shape}')
        # Выводит в логи типы данных каждого столбца DF
        logging.info(f'Типы данных:\n{df.dtypes}')

        logging.info('Связь с БД установлена')
        try:
            # Отправляет данные в заданную таблицу
            df.to_sql(f'{rout['bd']}', engine, if_exists='append', index=False)
        # Логирует любую ошибку(исключение) и записывает ее в переменную
        except Exception as e:
            logging.error(f'Ошибка при загрузки в БД {e}')
        logging.info('процесс ETL успешно завершен, данные загружены в БД')

# %%
    extract_args = extract.expand(rout=routs)
    transform_args = transform.expand(info=extract_args)
    load.expand(info=transform_args)
