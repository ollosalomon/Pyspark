from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('etl_pyspark', default_args=default_args, schedule_interval='@daily')

run_etl = BashOperator(
    task_id='run_etl',
    bash_command='spark-submit --master local /usr/local/airflow/scripts/etl.py',
    dag=dag,
)
