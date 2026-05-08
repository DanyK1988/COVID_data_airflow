import requests
import logging
from airflow import DAG
from datetime import datetime
from airflow.ptoviders.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook


BASE_ENDPOINT = 'CSSEGISandData/COVID-19/refs/heads/master/csse_covid_19_data/csse_covid_19_daily_reports/'

default_args = {
    'owner': 'Daniil',
    'email': 'daniilkalibera@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 60, # seconds
    'depends_on_past': False,
    'start_data': datetime(2026, 5, 5)
    'end_date': datetime(2026, 5, 9)
}

def download_covid_data(**kwargs):
    conn = BaseHook.get_connection(kwargs['conn_id'])
    url = conn.host + kwargs['endpoint'] + kwargs['exec_data'] + '.csv'
    logging.info(f'Sending request to {url}')
    response = requests.get(url)
    if response.status_code == 200:
        save_path = f'/opt/airflow/dags/files/{kwargs["exec_data"]}.csv'
        logging.info(f'Saving data to {save_path}')
        with open(save_path, 'wb') as f:
            f.write(response.text)
    else:
        raise ValueError(f'Unable to download data from {url}')


with DAG(
    dag_id='covid_daily_dag',
    tags=['daily', 'covid'],
    description='Covid daily data download',
    schedule_interval='0 7 * * *',
    max_active_runs=2,
    concurrency=4,
    default_args=default_args,
    user_defined_macros={
        'convert_data': lambda dt: dt.strftime('%m/%d/%Y'),
    }
) as main_dag:
    EXEC_DATA = '{{ convert_data(execution_data) }}'

    check_if_data_available = HttpSensor(
        task_id='check_if_data_available',
        http_conn_id = 'covid_api',
        endpoint =f'{BASE_ENDPOINT}{EXEC_DATA}.csv',
        poke_interval=60, # try to fetch data
        timeout=600, # 10 tries, 1 each min
        soft_fail=False,
        mode='reschedule'
    )

    download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_covid_data,
        op_args={
            'conn_id': 'covid_api',
            'endpoint': BASE_ENDPOINT,
            'exec_data': EXEC_DATA
        }
    )