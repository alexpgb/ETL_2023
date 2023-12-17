"""
— Зарегистрируйтесь в ОрепWeatherApi (https://openweathermap.org/api)
— Создайте ETL, который получает температуру в заданной вами локации, и
дальше делает ветвление:

• В случае, если температура больше 15 градусов цельсия — идёт на ветку, 
в которой есть оператор, выводящий на
экран «тепло»;
• В случае, если температура ниже 15 градусов, идёт на ветку с оператором,
 который выводит в консоль «холодно».

Оператор ветвления должен выводить в консоль полученную от АРI температуру.

— Приложите скриншот графа и логов работы оператора ветвленния.
"""

from airflow import  DAG
from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import json
import logging

dag_id = 'l7_s7_hw'

import json

ENV_VAR_NAME = 'OPENWEATHERMAP'
ENV_VAR_APPID_KEY_NAME = 'appid'
COORDINATES = {'lat': 55.16444, 'lon': 61.43684}
"""
перед стартом сервисов в режиме демона задать переменные окружения
export AIRFLOW_VAR_OPENWEATHERMAP='{"appid":"..."}'
"""
TEMP_THRESHOLD_COOL_HOT_KELVIN = 273

def get_env_var(env_var_name, env_var_key_name):
    env_avr = Variable.get(env_var_name, deserialize_json=True)
    return env_avr.get(env_var_key_name)


def format_endpoint_openweathermap(lat, lon, apikey):
    return f'/data/2.5/weather?lat={lat}&lon={lon}&appid={apikey}'
#   https://openweathermap.org/current - формирование endpoint и получение ответа

# def extract_data_from_response(**kwargs) -> dict:
#     response_data = kwargs['ti'].xcom_pull(task_ids='get_data')
#     return json.loads(response_data) # возвращает словарь  

def choose_branch_by_temperature(temp, temp_threshold):
    result = 'print_zero'
    if temp > temp_threshold:
        result = 'print_hot'
    elif temp < temp_threshold:
        result = 'print_cool'
    return result


with DAG (
    dag_id=dag_id,
    description='Homework in seminar 7 ETL course',
    start_date=datetime(2023, 1, 1),
#   https://stackoverflow.com/questions/53191639/airflow-set-dag-to-not-be-automatically-scheduled    
    schedule='@once',
    is_paused_upon_creation=True,
    catchup=False,
    tags=['Homework', 'Seminar 7', 'ETL'], 
    ) as dag:

    @task
    def print_settings():
        return f'\n{Variable.get(ENV_VAR_NAME, deserialize_json=True)}\nend'

    
    wait_for_api = HttpSensor(
        task_id='wait_for_api',
        http_conn_id='http_openweathermap_api',
        endpoint=format_endpoint_openweathermap(COORDINATES['lat'], \
                                          COORDINATES['lon'], \
                                            get_env_var(ENV_VAR_NAME, ENV_VAR_APPID_KEY_NAME)),
        response_error_codes_allowlist=['400', '404', '500', '503'],
        mode='poke',
        timeout=30,
        poke_interval=10,
        soft_fail=True,     # устанавливает эту и следующую задачу в статус skipped если возник failure (например вылет по таймауту 
        )

    get_data = SimpleHttpOperator(
        task_id='get_data',
        http_conn_id='http_openweathermap_api',
        endpoint=format_endpoint_openweathermap(COORDINATES['lat'], \
                                          COORDINATES['lon'], \
                                            get_env_var(ENV_VAR_NAME, ENV_VAR_APPID_KEY_NAME)),
        method='GET',
        )


    @task
    def extract_data_from_response(**kwargs) -> dict:
        response_data = kwargs['ti'].xcom_pull(task_ids='get_data')
        return json.loads(response_data) # возвращает словарь  
    

    @task
#    def extract_temperature_from_data(weather_data_as_json: str):
    def extract_temperature_from_data(weather_data_as_json:dict) -> int:
        temp = None
        if weather_data_as_json:
            try:
                temp = weather_data_as_json['main']['temp']
            except KeyError:
                pass
        return temp
    

    @task.branch
    def choose_branch_by_temperature(temp, temp_threshold):
        logging.info(f'Current temp: {temp}')
        result = 'print_zero'
        if temp > temp_threshold:
            result = 'print_hot'
        elif temp < temp_threshold:
            result = 'print_cool'
        return result

    extract_data_from_response_instance = extract_data_from_response()  

    # extract_temperature_from_data_instance = \
    #     extract_temperature_from_data(weather_data_as_json=extract_data_from_response_instance)

    extract_temperature_from_data_instance = \
        extract_temperature_from_data(weather_data_as_json=extract_data_from_response_instance)
    
    choose_branch_by_temperature_instance = \
        choose_branch_by_temperature(temp=extract_temperature_from_data_instance, temp_threshold=TEMP_THRESHOLD_COOL_HOT_KELVIN) 

    print_settings() >> wait_for_api >> get_data >> \
        extract_data_from_response_instance >> \
        extract_temperature_from_data_instance >> \
        choose_branch_by_temperature_instance 
        

    @task
    def print_hot():
        return 'Hotly!'

    @task
    def print_zero():
        return 'Zero!'


    @task
    def print_cool():
         return 'Coldly!'

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def print_finish():
        return 'Finish!'


    choose_branch_by_temperature_instance  >> \
             [print_hot(), print_zero(), print_cool()] >> print_finish()
