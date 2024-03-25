from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'generate_random_number',
    default_args=default_args,
    description='A simple DAG to generate a random number using BashOperator',
    schedule_interval='@daily',
)

generate_random_number = BashOperator(
    task_id='generate_random_number',
    bash_command='echo $((RANDOM % 100))',
    dag=dag,
)
