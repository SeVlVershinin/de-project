import psycopg2

from py_scripts.etl_tasks import load_data_into_dwh as op_load_data_into_dwh
from py_scripts.logger import logger
from py_scripts.report_generators import generate_reports as op_generate_reports

from datetime import timedelta, datetime
from typing import NoReturn

from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def get_db_connection():
    return psycopg2.connect(
        host=Variable.get('HOST'),
        port=Variable.get('PORT'),
        dbname=Variable.get('DB_NAME'),
        user=Variable.get('USER'),
        password=Variable.get('PASSWORD'),
    )


def init() -> NoReturn:
    logger.info('init start')
    current_date = datetime.now(),
    logger.ingo('initialized with current date %s', current_date)
    return current_date

def load_data_to_dwh() -> NoReturn:
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            op_load_data_into_dwh(datetime.now(), cursor)
            connection.commit()

def generate_reports() -> NoReturn:
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            op_generate_reports(datetime.now(), cursor)
            connection.commit()


dag = DAG(
    dag_id="etl-dag",
    schedule_interval='0 23 * * *',
    start_date=days_ago(2),
    catchup=False,
    tags=['etl', 'de'],
    default_args={
        'owner': 'Sergey Vershinin',
        'email': 'sevlvershinin@edu.hse.ru',
        'email_on_failure': True,
        'email_on_retry': False,
        'retry': 1,
        'retry-delay': timedelta(minutes=1)

    }
)

task_init = PythonOperator(task_id='init', python_callable=init, dag=dag)
task_load_data_to_dwh = PythonOperator(
    task_id='load_data_to_dwh',
    python_callable=load_data_to_dwh,
    dag=dag,
)
task_generate_reports = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports,
    dag=dag,
)
task_init >> task_load_data_to_dwh >> task_generate_reports
