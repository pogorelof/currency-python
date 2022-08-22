import psycopg2
from psycopg2 import sql
from datetime import timedelta, datetime
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

args = {
    "owner":"pogorelov",
    "retries":1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2022,1,1),
    "provide_context": True,
}

def extract(**kwargs):
    ti = kwargs['ti']
    conn = psycopg2.connect(database="airflow", user="airflow", password="airflow", host="postgres", port="5432")
    conn.autocommit = True
    cursor = conn.cursor()
    #получение данных с таблицы
    cursor.execute("SELECT json FROM currency")
    list_of_json = cursor.fetchall()
    ti.xcom_push(key = 'json', value = list_of_json)
    conn.close()

def transform(**kwargs):
    ti = kwargs['ti']
    list_of_json = ti.xcom_pull(key = 'json', task_ids = 'extract_data')
    conn = psycopg2.connect(database="airflow", user="airflow", password="airflow", host="postgres", port="5432")
    conn.autocommit = True
    cursor = conn.cursor()
    #получение всех валют в json для использования в следующем цикле
    list_of_currencies = list()
    for key in list_of_json[0][0].keys():
        list_of_currencies.append(key)
    #заполнение и фильтрование списка для последующей передачи в базу данных
    list_for_db = list()
    for currency_tuple in list_of_json:
        for currency_dict in currency_tuple:
            buf_tuple = tuple()
            for currencies in list_of_currencies:
                buf_tuple = (currencies, "EUR", currency_dict[currencies])
                list_for_db.append(buf_tuple)
    ti.xcom_push(key = 'json_db', value = list_for_db)
    conn.close()

def insert_in_table(**kwargs):
    ti = kwargs['ti']
    list_for_db = ti.xcom_pull(key = 'json_db', task_ids = "transform_data")
    conn = psycopg2.connect(database="airflow", user="airflow", password="airflow", host="postgres", port="5432")
    conn.autocommit = True
    cursor = conn.cursor()
    #вставка в базу данных
    insert = sql.SQL("INSERT INTO quotation (convert, base, rate) VALUES {}").format(
        sql.SQL(",").join(map(sql.Literal, list_for_db))
    )
    cursor.execute(insert)
    conn.close()


with DAG(
    dag_id = "base_and_target",
    schedule_interval = "@weekly",
    default_args = args,
    catchup = False
 ) as f:
    extract = PythonOperator(task_id = 'extract_data', python_callable = extract,)
    transform = PythonOperator(task_id = 'transform_data', python_callable = transform,)
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'datapg',
        sql = 
        """
                CREATE TABLE if not exists quotation(
                id serial,
                convert varchar(3),
                base varchar(3),
                rate decimal
                );
        """,
        )
    insert_in_table = PythonOperator(task_id = 'insert_in_table', python_callable = insert_in_table,)
    extract >> transform >> create_table >> insert_in_table