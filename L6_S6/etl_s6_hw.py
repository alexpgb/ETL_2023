"""
1. Создайте новый граф. Добавьте в него BashOperator, который будет генерировать рандомное число и печатать его в
консоль.

2. Создайте PythonOperator, который генерирует рандомное число, 
возводит его в квадрат и выводит в консоль исходное число и результат.

3. Сделайте оператор, который отправляет запрос к 
https://goweather.herokuapp.com/weather/""location"" (вместо location используйте ваше местоположение).

4. Задайте последовательный порядок выполнения операторов.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
from airflow.decorators import dag, task
from datetime import datetime
from random import randint
import json

ENV_VAR_NAME = 'OPENWEATHERMAP'
ENV_VAR_APPID_KEY_NAME = 'appid'
COORDINATES = {'lat': 55.16444, 'lon': 61.43684}


def print_random_var_square():
    num = randint(0, 100)
    return f'Random number: {num}, its square {num**2}.'


def get_env_var(env_var_name, env_var_key_name):
    env_avr = Variable.get(env_var_name, deserialize_json=True)
    return env_avr.get(env_var_key_name)


def format_endpoint_openweathermap(lat, lon, apikey):
    return f'/data/2.5/weather?lat={lat}&lon={lon}&appid={apikey}'


def print_response(**kwargs):
    response_data = kwargs['ti'].xcom_pull(task_ids='get_data')
    return json.loads(response_data)

@dag(
    description='Homework in seminar 6 ETL course',
    start_date=datetime(2023, 1, 1),
    schedule='@once',
    is_paused_upon_creation=True,
    catchup=False,
    tags=['Homework', 'Seminar 6', 'ETL'],
    )
def l6_s6_hw():

    print_random_number_by_bash = BashOperator(
        task_id = 'print_random_numeric_by_bash',
        bash_command='echo $RANDOM'
        )
    
    print_random_number_square_python = PythonOperator(
        task_id='print_random_number_square_python',
        python_callable=print_random_var_square
        )

    get_data = SimpleHttpOperator(
        task_id='get_data',
        http_conn_id='http_openweathermap_api',
        endpoint=format_endpoint_openweathermap(COORDINATES['lat'], \
                                          COORDINATES['lon'], \
                                            get_env_var(ENV_VAR_NAME, ENV_VAR_APPID_KEY_NAME)),
        method='GET',
        )
    
    print_data = PythonOperator(
        task_id='print_data',
        python_callable=print_response,
        provide_context=True,
    )

    print_random_number_by_bash >> print_random_number_square_python >> get_data >> print_data

l6_s6_hw()
