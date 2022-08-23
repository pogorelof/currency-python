import requests
import json 
import psycopg2
from psycopg2 import sql
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

args = {
    "owner":"pogorelov",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2022,1,1),
    "provide_context": True
}

def extract(**kwargs):
    ti = kwargs['ti']
    #получение всех валют
    url = 'https://api.exchangerate.host/latest'
    response = requests.get(url)
    json_from_api = response.json()['rates']
    ti.xcom_push(key = 'json', value = json_from_api)

def transform(**kwargs):
    ti = kwargs['ti']
    json_from_api = ti.xcom_pull(key = 'json', task_ids = 'extract_data')
    #фильтрация для 1 таблицы
    dictionary = {
        'KZT': json_from_api['KZT'],
        'RUB': json_from_api['RUB'],
        'BTC': json_from_api['BTC']
    }
    json_to_db = json.dumps(dictionary, indent=4)
    ti.xcom_push(key = 'json_db', value = json_to_db)
    #фильтрация для 2 таблицы
    list_to_db = list()
    list_to_db.append(tuple(("KZT", "EUR", json_from_api['KZT'])))
    list_to_db.append(tuple(("RUB", "EUR", json_from_api['RUB'])))
    list_to_db.append(tuple(("BTC", "EUR", json_from_api['BTC'])))
    ti.xcom_push(key = 'list_db', value = list_to_db)

def insert_in_table_currency(**kwargs):
    ti = kwargs['ti']
    conn = psycopg2.connect(database="airflow", user="airflow", password="airflow", host="postgres", port="5432")
    conn.autocommit = True
    cursor = conn.cursor()
    json_to_db = ti.xcom_pull(key='json_db', task_ids='transform_data')
    insert = f"INSERT INTO currency(json, date) VALUES('{ json_to_db }', current_timestamp);"
    cursor.execute(insert)
    conn.close()

def insert_in_table_quotation(**kwargs):
    ti = kwargs['ti']
    conn = psycopg2.connect(database="airflow", user="airflow", password="airflow", host="postgres", port="5432")
    conn.autocommit = True
    cursor = conn.cursor()
    list_to_db = ti.xcom_pull(key='list_db', task_ids='transform_data')
    insert = sql.SQL("INSERT INTO quotation (convert, base, rate) VALUES {}").format(
        sql.SQL(",").join(map(sql.Literal, list_to_db))
    )
    cursor.execute(insert)
    conn.close()

with DAG(
    dag_id = "currency",
    schedule_interval = "*/20 10-21 * * MON-FRI",
    default_args = args,
    catchup=False
    ) as f:
    extract = PythonOperator(task_id = 'extract_data', python_callable = extract,)
    transform = PythonOperator(task_id = 'transform_data', python_callable = transform,)
    insert_in_table_currency = PythonOperator(task_id = "insert_in_table_currency", python_callable = insert_in_table_currency)
    insert_in_table_quotation = PythonOperator(task_id = "insert_in_table_quotation", python_callable = insert_in_table_quotation)
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'datapg',
        sql = 
        """
                CREATE TABLE if not exists currency(
                id serial,
                json json,
                date timestamp with time zone
                );
                CREATE TABLE if not exists quotation(
                id serial,
                convert varchar(3),
                base varchar(3),
                rate decimal
                );
        """,
        )
    extract >> transform >> create_table >> insert_in_table_currency >> insert_in_table_quotation