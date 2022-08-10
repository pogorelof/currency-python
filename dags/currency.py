from urllib import response
import requests
import json 
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
    #фильтрация
    dictionary = {
        'KZT': json_from_api['KZT'],
        'RUB': json_from_api['RUB'],
        'BTC': json_from_api['BTC']
    }
    json_to_db = json.dumps(dictionary, indent=4)
    ti.xcom_push(key = 'json_db', value = json_to_db)


with DAG(
    dag_id = "currency",
    schedule_interval = "*/20 10-21 * * MON-FRI",
    default_args = args,
    catchup=False
    ) as f:
    extract = PythonOperator(task_id = 'extract_data', python_callable = extract,)
    transform = PythonOperator(task_id = 'transform_data', python_callable = transform,)
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
        """,
        )
    insert_in_table = PostgresOperator(
        task_id = 'insert_in_table',
        postgres_conn_id = 'datapg',
        sql=
        """
                INSERT INTO currency(json, date) VALUES(
                '{{ ti.xcom_pull(key='json_db', task_ids='transform_data') }}',
                current_timestamp
                );
        """
        )
    extract >> transform >> create_table >> insert_in_table