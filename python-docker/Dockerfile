FROM python

WORKDIR /app

RUN pip install psycopg2

RUN pip install urllib3

RUN pip install requests 

COPY . .

CMD ["python", "currency.py"]