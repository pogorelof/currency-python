# Currency
This application collects the data of three currencies: KZT, RUB and BTC. Fresh data is added to the table every 20 minutes from Monday to Friday from 10:00 to 21:00.

## Launch
You need Docker to run and PostgreSQL. <br>
1) Being in the project directory, you need to write: <br>
```docker-compose up -d --build```
2) In a short amount of time will be launched webserver, where will Airflow UI be. webserver is running at: <br>
```localhost:8080```
3) When database initialization finished: <br>
``` docker exec currency-python_webserver_1 bash -c "airflow connections -a --conn_uri 'postgres://airflow:airflow@postgres:5432/airflow' --conn_id datapg" ```
4) From the UI you can start the process. 
5) To connect to a database: <br>
```
host: localhost
port: 3307
user: airflow
database: airflow
password: airflow
```
6) Data is added to the table: **currency** <br>
``select * from currency ``
7) Second table **quotation** shows target/base currency and their ratio <br>
``select * from quotation ``
```
The second table takes data from the first table, so you need to run the first "currency" dag first
```