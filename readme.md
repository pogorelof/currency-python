# Currency
This application shows the data of three currencies: KZT, RUB and BTC. Fresh data is added to the table every 20 minutes from Monday to Friday from 10:00 to 21:00.

## Launch
You need Docker to run and PostgreSQL. <br>
1) Being in the project directory, you need to write: <br>
```docker-compose up -d --build```
2) In a short amount of time will be launched webserver, where will Airflow UI be. webserver is running at: <br>
```localhost:8080```
3) In the UI, you need to go to: Admin -> Connections. Press "Create" and type:
```
Conn Id: datapg
Conn Type: Postgres
Host: postgres
Schema: airflow
Host: airflow
Password: airflow
Port: 5432
Extra: clear
```
3) From the UI you can start the process. 
4) To connect to a database: <br>
```
host: localhost
port: 3307
user: airflow
database: airflow
password: airflow
```
5) Data is added to the table: **currency**