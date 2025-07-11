from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG, models
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from datetime import datetime, timedelta
import logging
import clickhouse_connect
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


def connect_to_ch():  # Получает соединение с ClickHouse
    conn_ch = clickhouse_connect.get_client(
        host='clickhouse',         # или IP ClickHouse
        port=8123,                # стандартный порт для HTTP
        username='default',
        password='default',
        database='airflare'
    )
    return conn_ch


def save_to_s3(data, key):  # Сохраняет DataFrame в S3 в формате JSON, data=DataFrame, key=путь.json
    hook = S3Hook(aws_conn_id='minio')
    data = data.to_json(index=False, orient='records', date_format='iso')

    try:
        hook.load_string(  # Загружает строку в S3
            string_data=data,  # Сериализованные в строку данные
            key=key,  # Путь до файла в заданном бакете
            bucket_name='airflare',  # Название бакета
            replace=True
        )
    except Exception as e:
        logging.error(f'Ошибка {e}')


def load_from_s3(key):  # Выгружает JSON из S3 и возвращает DataFrame, в качесте аргумента нужно получить путь до файла
    logging.info('Начинаю загрузку из S3')
    hook = S3Hook(aws_conn_id='minio')

    try:
        # Проверяет есть ли бакет с таким именем
        if not hook.check_for_bucket('airflare'):
            hook.create_bucket(bucket_name='airflare')
        logging.info('Подключение к S3 установлено')
    except Exception as e:
        logging.error(f'Ошибка подключения к minio, {e}')

    try:
        logging.info('Начинаю чтение')
        file = hook.read_key(bucket_name='airflare', key=key)
        json_file = json.loads(file)
        df = pd.DataFrame(json_file)
        logging.info('Файл успешно прочитан, возвращаю DataFrame')
        return df
    except Exception as e:
        logging.error(f'Ошибка {e}')


# Выгружает дату предыдущей загрузки данных в CH для частичной загрузки данных из postgres, table= название таблицы
def last_date_of_extraction(table):
    logging.info('Узнаю дату последнего внесения данных в CH')
    try:
        result = connect_to_ch().query(f'''
                                    SELECT max(date_of_extraction)
                                    FROM {table}'''
                                       )
        max_date = result.result_rows[0][0]
        logging.info(
            f'Данные получены, в ClickHouse будут внесены данные позднее {pd.to_datetime(max_date) if max_date else pd.to_datetime("2000-01-01")}')
        # если данных в CH нет, то возвращает другую дату
        return pd.to_datetime(max_date) if max_date else pd.to_datetime("2000-01-01")
    except Exception as e:
        logging.error(f'Ошибка извлечения даты из CH, {e}')


with DAG(dag_id='load_data_to_ClickHouse', default_args=default_args, schedule_interval='0 13 * * *', catchup=False, tags=['load_CH']) as dag:

    @task
    def load_from_postgres():  # Выгружает данные из postgres
        logging.info('Начинаю подключение к postgres')
        # Создает хук к postgres
        hook = PostgresHook(postgres_conn_id='postgres_airfare')

        last_date_of_outbound = last_date_of_extraction(
            'ikt_airflare_outbound')
        last_date_of_return = last_date_of_extraction('ikt_airflare_return')

        rows_outbound = hook.get_pandas_df(
            '''SELECT iar.id, depart_date, ic.iata AS iata_origin, ic.city_name AS city_name_origin, ic.country AS country_origin, ic_1.iata AS iata_destination, ic_1.city_name AS city_name_destination, ic_1.country AS country_destination, trip_class, value, gate, duration, distance, number_of_changes, date_of_extraction, day_before_departure
               FROM ikt_airflare_outbound AS iar JOIN iata_codes AS ic ON iar.origin = ic.id
			   JOIN iata_codes AS ic_1 ON iar.destination = ic_1.id
               WHERE date_of_extraction > %s''', parameters=(last_date_of_outbound,))

        rows_return = hook.get_pandas_df('''SELECT iar.id, depart_date, ic.iata AS iata_origin, ic.city_name AS city_name_origin, ic.country AS country_origin, ic_1.iata AS iata_destination, ic_1.city_name AS city_name_destination, ic_1.country AS country_destination, trip_class, value, gate, duration, distance, number_of_changes, date_of_extraction, day_before_departure
                                FROM ikt_airflare_return AS iar JOIN iata_codes AS ic ON iar.origin = ic.id
								JOIN iata_codes AS ic_1 ON iar.destination = ic_1.id
                                WHERE date_of_extraction > %s''', parameters=(last_date_of_return,))

        save_to_s3(data=rows_return, key='ClickHouse_data/rows_return.json')
        save_to_s3(data=rows_outbound,
                   key='ClickHouse_data/rows_outbound.json')

    @task
    def load_to_ClickHouse():  # Сохраняет данные в ClickHouse
        logging.info('Начинаю загрузку в ClickHouse')
        # Выгружает из S3 полученные данные из postgres
        return_table = load_from_s3('ClickHouse_data/rows_return.json')
        outbound_table = load_from_s3('ClickHouse_data/rows_outbound.json')

        logging.info('Начинаю восстановление дат')
        try:  # Восстанавливаем правильный тип данных в колонках с датой
            return_table['depart_date'] = pd.to_datetime(
                return_table['depart_date'])
            return_table['date_of_extraction'] = pd.to_datetime(
                return_table['date_of_extraction'])

            outbound_table['depart_date'] = pd.to_datetime(
                outbound_table['depart_date'])
            outbound_table['date_of_extraction'] = pd.to_datetime(
                outbound_table['date_of_extraction'])
        except Exception as e:
            logging.error(f'Данные отсутствуют, ошибка {e}')
            raise FileNotFoundError
        print(return_table.dtypes)
        print(outbound_table.dtypes)
        logging.info('Даты нормализованы, начинаю отправку в ClickHouse')
        try:  # Отправляем данные в CH
            connect_to_ch().insert_df(table='ikt_airflare_return', df=return_table)
            connect_to_ch().insert_df(table='ikt_airflare_outbound', df=outbound_table)
        except Exception as e:
            logging.error(f'Ошибка загрузки в БД, {e}')

    task1 = load_from_postgres()
    task2 = load_to_ClickHouse()

    task1 >> task2
