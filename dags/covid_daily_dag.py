import requests
import logging
from airflow import DAG
from datetime import datetime
from airflow.ptoviders.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os


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
        directory = '/opt/airflow/data'
        save_path = os.path.join(directory, f"{kwargs['exec_data']}.csv")
        os.makedirs(directory, exist_ok=True)
        logging.info(f'Saving data to {save_path}')
        with open(save_path, 'wb') as f:
            f.write(response.content)
        return save_path
    else:
        raise ValueError(f'Unable to download data from {url}')


def upload_to_hdfs(exec_data):
    hdfs_hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')

    local_path = f'/opt/airflow/data/{exec_data}.csv'
    remote_dir = '/covid_data/csv'
    remote_path = f'{remote_dir}/{exec_data}.csv'

    # Checking/creating folder in HDFS
    hdfs_hook.get_conn().mkdir(remote_dir)

    # 2. Uploading the file
    hdfs_hook.load_file(
        file_path=local_path,
        remote_path=remote_path,
        overwrite=True
    )

    # 3. Delete local file
    if os.path.exists(local_path):
        os.remove(local_path)


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
    EXEC_DATE = '{{ convert_data(execution_data) }}'

    check_if_data_available = HttpSensor(
        task_id='check_if_data_available',
        http_conn_id = 'covid_api',
        endpoint =f'{BASE_ENDPOINT}{EXEC_DATE}.csv',
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
            'exec_data': EXEC_DATE
        }
    )

    move_to_hdfs = PythonOperator(
        task_id='move_to_hdfs',
        python_callable=upload_to_hdfs,
        op_kwargs={'exec_data': EXEC_DATE}
    )

    process_data = SparkSubmitOperator(
        task_id='process_data',
        application=os.path.join(main_dag.folder, 'scripts/covid_data_processing.py'),
        conn_id='spark_conn',
        name=f'{main_dag.dag_id}.process_data',
        application_args=[
            '--exec_date', EXEC_DATE
        ]
    )