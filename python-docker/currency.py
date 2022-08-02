from urllib import response
import psycopg2
import requests
import json 

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

#соединение с базой данных
connect = psycopg2.connect(
    database = "postgres",
    user = "postgres",
    password = "qwerty",
    host = "localhost",
    port = "5432"
)

cur = connect.cursor()
#запрос на создание таблицы
try:
    cur.execute(
        'CREATE TABLE currency('+
        'id serial,'+
        'json json,'+
        'date timestamp with time zone'+
        ');'
    )   
except (Exception) as error:
    print("Таблица уже создана")
    
#запрос на добавление новой строки в бд
try:
        cur.execute(
        'INSERT INTO currency(json, date) VALUES (\'' + json_to_db + '\', current_timestamp);'
    )
except:
    print("Ошибка при добавлении")
connect.commit()  
connect.close()