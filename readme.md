# Currency
This application shows the data of three currencies: KZT, RUB and BTC. Fresh data is added to the table every minute.

## Launch
You need Docker to run and PostgreSQL. <br>
1) Being in the project directory, you need to write: <br>
```docker-compose up -d --build```
2) In a short amount of time will be launched webserver, where will Airflow UI be. webserver is running at: <br>
```localhost:8080```
3) From the UI you can start the process. 
4) To connect to a database: <br>
```
host: localhost
port: 3307
user: airflow
database: airflow
password: airflow
```