import datetime
import time
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import os



args = {
    'owner': 'Igor',
    'start_date': datetime.datetime(2022, 2, 14),
    'provide_context': True
}

def extract_data(**kwargs):
    ti = kwargs['ti']
    response = requests.get(
        f'http://quotes.toscrape.com/page/1/',)
    if response.status_code == 200:
        # Разбор ответа сервера
        json_data = str(response.text)
        print(json_data)
        ti.xcom_push(key='test', value=json_data)


def transform_data(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key='test', task_ids=['extract_data'])[0]
    ti.xcom_push(key='test_data', value=json_data)



def load_data(**kwargs):
    print("ANUS")





with DAG('load_weather_wwo', description='load_weather_wwo', schedule_interval='*/1 * * * *', catchup=False,
         default_args=args) as dag:  # 0 * * * *   */1 * * * *
    extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data)
    transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data)
    load_data = PythonOperator(task_id='load_data', python_callable=load_data,)


    extract_data >> [transform_data, transform_data] >> load_data

