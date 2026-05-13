import requests
import logging
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.providers.apache.hive.operators.hive import HiveOperator
import os


BASE_ENDPOINT = 'CSSEGISandData/COVID-19/refs/heads/master/csse_covid_19_data/csse_covid_19_daily_reports/'
HIVE_TABLE = 'COVID_RESULTS'

default_args = {
    'owner': 'Daniil',
    'email': 'daniilkalibera@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=60),
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 5),
    'end_date': datetime(2026, 5, 9)
}

def download_covid_data(**kwargs):
    conn = BaseHook.get_connection(kwargs['conn_id'])
    host = conn.host.rstrip("/")
    url = f"{host}/{kwargs['endpoint']}{kwargs['api_date']}.csv"
    logging.info(f'Sending request to {url}')
    response = requests.get(url)
    if response.status_code == 200:
        directory = '/opt/airflow/data'
        save_path = os.path.join(directory, f"{kwargs['exec_date']}.csv")
        os.makedirs(directory, exist_ok=True)
        logging.info(f'Saving data to {save_path}')
        with open(save_path, 'wb') as f:
            f.write(response.content)
        return save_path
    else:
        raise ValueError(f'Unable to download data from {url}')


def upload_to_hdfs(exec_date):
    hdfs_hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')

    local_path = f'/opt/airflow/data/{exec_date}.csv'
    remote_dir = '/covid_data/csv'
    remote_path = f'{remote_dir}/{exec_date}.csv'

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


def check_if_table_exists(**kwargs):
    table = kwargs['table_name'].lower()
    conn = HiveCliHook(hive_cli_conn_id=kwargs['conn_id'])
    logging.info(f'Checking if table {table} exists')
    query = f"SHOW TABLES in default like '{table}';"
    logging.info('Running the query')
    result = conn.run_cli(hql=query)
    if table in result:
        logging.info(f'Table {table} exists')
        return 'load_to_hive'
    logging.info(f'Table {table} does not exist')
    return 'create_hive_table'




with DAG(
    dag_id='covid_daily_dag',
    tags=['daily', 'covid'],
    description='Covid daily data download',
    schedule='0 7 * * *',
    max_active_runs=1,
    default_args=default_args,
    user_defined_macros={
        'convert_date': lambda dt: dt.strftime('%m/%d/%Y'),
    }
) as main_dag:
    API_DATE = '{{ convert_date(logical_date) }}'
    EXEC_DATE = '{{ logical_date.strftime("%Y-%m-%d") }}'

    check_if_data_available = HttpSensor(
        task_id='check_if_data_available',
        http_conn_id = 'covid_api',
        endpoint =f'{BASE_ENDPOINT}{API_DATE}.csv',
        poke_interval=60, # try to fetch data
        timeout=600, # 10 tries, 1 each min
        soft_fail=False,
        mode='reschedule'
    )

    download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_covid_data,
        op_kwargs={
            'conn_id': 'covid_api',
            'endpoint': BASE_ENDPOINT,
            'api_date': API_DATE,
            'exec_date': EXEC_DATE
        }
    )

    move_to_hdfs = PythonOperator(
        task_id='move_to_hdfs',
        python_callable=upload_to_hdfs,
        op_kwargs={'exec_date': EXEC_DATE}
    )

    process_data = SparkSubmitOperator(
        task_id='process_data',
        application=os.path.join(main_dag.folder, 'scripts/covid_data_processing.py'),
        conn_id='spark_conn',
        name=f'{main_dag.dag_id}.process_data',
        application_args=[
            '--exec_data', EXEC_DATE
        ]
    )
    check_if_hive_table_exists = BranchPythonOperator(
        task_id='check_if_hive_table_exists',
        python_callable=check_if_table_exists,
        op_kwargs={
            'table_name': HIVE_TABLE,
            'conn_id': 'hive_conn'
        }
    )
    create_hive_table = HiveOperator(
        task_id='create_hive_table',
        hive_cli_conn_id='hive_conn',
        hql="""
        CREATE EXTERNAL TABLE IF NOT EXISTS default.{table}(
            country_region STRING,
            total_confirmed INT,
            total_deaths INT,
            fatality_ratio DOUBLE,
            world_case_pct DOUBLE,
            world_death_pct DOUBLE
            )
            PARTITIONED BY (exec_date STRING)
            ROW FORMAT DELIMITED
            FIELD TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION '/covid_data/results';
            """.format(table=HIVE_TABLE)
    )
    load_to_hive = HiveOperator(
        task_id='load_to_hive',
        hive_cli_conn_id='hive_conn',
        hql=f"""MSCK REPAIR TABLE default.{HIVE_TABLE};""",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_if_data_available >> download_data >> move_to_hdfs >> process_data >> check_if_hive_table_exists
    check_if_hive_table_exists >> [create_hive_table, load_to_hive]
    create_hive_table >> load_to_hive
