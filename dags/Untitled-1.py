# %%
import requests
import pandas as pd
# Устанавливает соединение с postgres
from airflow.providers.postgres.hooks.postgres import PostgresHook
# Устанавливает соединение с S3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG, models
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging  # Логирование
import json  # Сериализация и десириализация json в строку для загрузки в S3

# %%
# Извлечение IATA кодов из БД
hook = PostgresHook(postgres_conn_id='postgres_airfare')
engine = hook.get_sqlalchemy_engine()
iata_df = pd.read_sql('SELECT id, iata FROM iata_codes', con=engine)
iata_dict = dict(zip(iata_df['iata'], iata_df['id']))

# %%
city_list = list(iata_df['iata'])  # Города назначения
routs = []

for i in range(1, len(city_list)):  # Генерирует комбинацию билетов туда-обратно
    city_dict = {}
    city_dict['origin'] = city_list[0]
    city_dict['destination'] = city_list[i]
    city_dict['bd'] = 'ikt_airflare_outbound'
    routs.append(city_dict)

    city_dict = {}
    city_dict['origin'] = city_list[i]
    city_dict['destination'] = city_list[0]
    city_dict['bd'] = 'ikt_airflare_return'
    routs.append(city_dict)


# %%
# Функция загружает данные в S3, data-файл для загрузки, bucket - имя бакета, key - путь и название файла
def upload_to_s3(data, key, bucket='airflare'):
    logging.info('Создание подключения к S3')
    hook = S3Hook(aws_conn_id='minio')  # Установка подключения к S3

    try:
        # Проверяет есть ли бакет с таким именем
        if not hook.check_for_bucket(bucket):
            hook.create_bucket(bucket_name=bucket)
        logging.info('Подключение установлено')
    except Exception as e:  # Логирование ошибки
        logging.error(f'Ошибка подключения к minio, {e}')

    logging.info('Загрузка данных в S3')

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
    hook = S3Hook(aws_conn_id=aws_conn_id)
    try:
        # Проверяет есть ли бакет с таким именем
        if not hook.check_for_bucket(bucket):
            hook.create_bucket(bucket_name=bucket)
        logging.info('Подключение установлено')
    except Exception as e:
        logging.error(f'Ошибка подключения к minio, {e}')

    logging.info('Подключение установлено, загрузка данных из S3')

    # Читает сериализованный файл в заданном бакете по заданному пути
    content = hook.read_key(bucket_name=bucket, key=key)

    data = json.loads(content)  # Десериализует файл
    logging.info('Данные выгружены из S3')
    return data


# %%
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
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
        path = upload_to_s3(data=json.dumps(response.json(
        )), key=f"row_airflare_data_by_day/response_{rout['origin']}_{rout['destination']}_{str(datetime.today().date())}.json")

        logging.info('Данные успешно сохранены в S3')

        return {'path': path, 'rout': rout}

# %%
    @task
    def transform(info):  # Функция оставляет только нужные нам данные
        path = info['path']
        rout = info['rout']
        logging.info('Извлечение данных из S3, подготовка к обработке')

        # Выгружает файл из S3 и десериализует его
        file = load_from_s3(bucket=path['bucket'], key=path['key'])

        logging.info('Начата обработка данных')

        df = pd.DataFrame(file['data'])
        df = df[['depart_date', 'origin', 'destination', 'trip_class',
                'value', 'gate', 'duration', 'distance', 'number_of_changes']]

        cur_date = pd.Series([datetime.date(datetime.today())] * len(
            # Создает серию хранящую текущую дату
            df), name='date_of_extraction').astype('str')

        # Создает серию хранящую время до вылета
        day_before_departure = pd.to_datetime(
            df['depart_date']) - pd.to_datetime(cur_date)
        day_before_departure.name = 'day_before_departure'
        day_before_departure = day_before_departure.map(lambda x: x.days)

        df = pd.concat([df, cur_date, day_before_departure], axis=1)
        # замена IATA кодов на индексы для нормализации данных
        df['origin'] = df['origin'].map(iata_dict)
        df['destination'] = df['destination'].map(iata_dict)

        # Очищает DataFrame от случайно попавших IATA кодов
        df.dropna(subset=['origin', 'destination'], inplace=True)

        logging.info(df['date_of_extraction'].dtype)
        logging.info(df['depart_date'].dtype)

        logging.info('Обработка завершена, начинается передача DF в S3')

        # Преобразует DF в JSON
        content = df.to_json(index=False, orient='records')

        path = upload_to_s3(
            # Загружает в S3
            content, bucket=path['bucket'], key=f"DataFrames/df_{rout['origin']}_{rout['destination']}.json")

        logging.info('Данные успешно сохранены в S3')

        return {'path': path, 'rout': rout}

# %%
    @task
    def load_to_postgres(info):  # Функция загружает данные в БД
        path = info['path']  # Путь до файла в S3
        rout = info['rout']  # Текущее направление
        # Выгружает из S3 файл который внутри содержит список словарей
        content = load_from_s3(bucket=path['bucket'], key=path['key'])

        logging.info(
            'Данные получены из хранилища, начат процесс отправки в БД')

        df = pd.DataFrame(list(content))

        # Указывать имя в airflow UI при соединении с БД которое указано в yaml(postgres_conn_id= это id в aiflow UI)
        hook = PostgresHook(postgres_conn_id='postgres_airfare')
        engine = hook.get_sqlalchemy_engine()
        logging.info('Проверяю подключение к postgres')
        try:  # Проверяем подключение к postgres
            # Получаем подключение
            conn = hook.get_conn()

            # Создаем курсор для выполнения простого sql запроса для проверки соединения
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1;")  # Выполняет запрос к БД
                result = cursor.fetchone()  # Возвращает результат запроса
                print("Соединение с PostgreSQL успешно. Результат запроса:", result)
        except Exception as e:
            print("Ошибка соединения с PostgreSQL:", e)

        # Выводит в логи размерность DF
        logging.info(f'Размер DataFrame: {df.shape}')
        # Выводит в логи типы данных каждого столбца DF
        logging.info(f'Типы данных:\n{df.dtypes}')

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
    load_to_postgres.expand(info=transform_args)
