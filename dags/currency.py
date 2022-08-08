from urllib import response
import psycopg2
import requests
import json 
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    "owner":"pogorelov",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2022,1,1),
}

def create_table():
    connect = psycopg2.connect(
        database = "airflow",
        user = "airflow",
        password = "airflow",
        host = "postgres",
        port = "5432"
    )
    cursor = connect.cursor()
    cursor.execute(
        'CREATE TABLE if not exists currency('+
        'id serial,'+
        'json json,'+
        'date timestamp with time zone'+
        ');'
    )
    connect.commit()  
    connect.close()

def get_currency():
    connect = psycopg2.connect(
        database = "airflow",
        user = "airflow",
        password = "airflow",
        host = "postgres",
        port = "5432"
    )
    cursor = connect.cursor()

    #получение всех валют
    url = 'https://api.exchangerate.host/latest'
    response = requests.get(url)
    json_from_api = response.json()

    #фильтрация
    json_from_api = json_from_api['rates']
    dictionary = {
        'KZT': json_from_api['KZT'],
        'RUB': json_from_api['RUB'],
        'BTC': json_from_api['BTC']
    }
    json_to_db = json.dumps(dictionary, indent=4)

    #запрос на добавление новой строки в бд
    cursor.execute(
        'INSERT INTO currency(json, date) VALUES (\'' + json_to_db + '\', current_timestamp);'
    )
    connect.commit()
    connect.close() 


with DAG(
    dag_id = "currency",
    schedule_interval = "*/1 * * * *",
    default_args = args,
    catchup=False
    ) as f:
    create_table = PythonOperator(
        task_id = "CreateTable",
        python_callable = create_table,
    )
    get_currency = PythonOperator(
        task_id = "GetCurrency",
        python_callable = get_currency,
    )

    create_table >> get_currency