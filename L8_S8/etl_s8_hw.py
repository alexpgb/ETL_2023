from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator, ExternalPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from pathlib import Path
import logging


FILES_NAME_FOR_DOWNLOAD = {
                           'hotel':{'file': 'hotel.csv'},
                           'clients': {'file': 'clients.csv'},
                           'booking': {'file': 'booking.csv'},
                            }
FILES_PROCESSORS = []
URL_DOWNLOAD_FROM = 'https://raw.githubusercontent.com/alexpgb/ETL_2023/main/L8_S8'
DIR_DOWNLOAD_TO = '/home/gbss/course_de_etl/HW8/downloads'
DIR_MERGED_TO = '/home/gbss/course_de_etl/HW8/merged'
FILE_MERGED_TO = 'booking_merged.csv'
PATH_TO_PYTHON_FOR_TRANSFORM = '/home/gbss/course_de_etl/pd/bin/python'

DOWNLOAD_FILES = True
MERGE_FILES = True
LOAD_DATA = True

def check_downloaded_files(**kwargs):
    dir_download_to = kwargs['dir_download_to']
    files_name_for_download = kwargs['files_name_for_download']
    for file in files_name_for_download:
        full_name = str(Path(Path(dir_download_to) / file))
        is_exists = Path(full_name).exists()
        logging.info(f'{full_name} is exists {is_exists}')
        if not is_exists:
            return False
    return True


@dag(
    description='Homework in seminar 8 ETL course',
    start_date=datetime(2023, 1, 1),
#   https://stackoverflow.com/questions/53191639/airflow-set-dag-to-not-be-automatically-scheduled    
    schedule='@once',
    is_paused_upon_creation=True,
    catchup=False,
    tags=['Homework', 'Seminar 8', 'ETL'], 
    )


def l8_s8_hw():

    tasks_downloads = []
    if DOWNLOAD_FILES:
        for key, val in FILES_NAME_FOR_DOWNLOAD.items():
            downloads = BashOperator(
                task_id=f'download_{key}',
                bash_command=f'curl -sSLo {DIR_DOWNLOAD_TO}/{val["file"]} '\
                    f'{URL_DOWNLOAD_FROM}/{val["file"]}',
                )
            tasks_downloads.append(downloads)
    else:
        downloads = EmptyOperator(task_id="skip_download")
        tasks_downloads.append(downloads)

    files_is_downloaded = ShortCircuitOperator(
        task_id="condition_is_True",
        python_callable=check_downloaded_files,
        op_kwargs={'dir_download_to':DIR_DOWNLOAD_TO, \
                'files_name_for_download':[val["file"] for val in FILES_NAME_FOR_DOWNLOAD.values()]}, # список наименований файлов
        )   

    # В литературе по Airflow не рекомендуют передвать обрабатываемую информацию между задачами напрямую т.к.:
    # 1. Есть ограничения на объем передаваемой информации.
    # 2. Возможны ошибки связанные с исчерпанием памяти.
    # 3. Это не совпадает с иделологией использования Airflow как оркестратора а не обработчика данных.
    # Поэтому принято решение передавать не данные а ссылки на скачанные файлы.

    # Реализуем запуск обработки в отдельном окружении, чтобы избежать зависимостей Airflow 
    # и зависимостей, необходимых для обработки данных

    if MERGE_FILES:
        @task.external_python(task_id='run_transform', python=PATH_TO_PYTHON_FOR_TRANSFORM)
        def run_transform(path_to_source_files: str, source_files: dict, path_to_target_file: str, target_file: str):
            import sys
            import os
            sys.path.insert(0, '/home/gbss/airflow/dags')
            # import os
            # os.chdir('/home/gbss/airflow/dags')
            from pythonscripts.processdata.procdata import transform
            result_code, message = transform(path_to_source_files, source_files, path_to_target_file, target_file)
            return
            # return os.path.abspath(__file__), os.getcwd(), sys.path
        run_transform_instance = run_transform(DIR_DOWNLOAD_TO, FILES_NAME_FOR_DOWNLOAD, DIR_MERGED_TO, 'booking_target.csv')
    else:
        # @task(task_id='skip_transform')
        # def run_transform(path_to_source_files: str, source_files: dict, path_to_target_file: str, target_file: str):
        #     return
        run_transform_instance = EmptyOperator(task_id="skip_transform")

    if LOAD_DATA:
        @task(task_id='load_data')
        def load_data(path_to_target_file: str, target_file: str)->None:
            query = "copy de.booking from stdin with (format csv, header true)"
            # try:
            postgres_hook = PostgresHook(postgres_conn_id="postgresql_local_postgres")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            with open(Path(path_to_target_file) / target_file, 'r') as file:    
                cur.copy_expert(query, file)
            conn.commit()
            return 0, '' 
            # except Exception as e:
            #     return 1, f'{e}'        
        load_data_isinstance = load_data(DIR_MERGED_TO,'booking_merged.csv')
    else:
        load_data_isinstance = EmptyOperator(task_id="skip_load")


    # @task
    # def f_empty():
    #     print('')

#    [task_download_hotel] >> files_is_downloaded >> merge_data()
    tasks_downloads >> files_is_downloaded >> run_transform_instance >> load_data_isinstance

l8_s8_hw()    
