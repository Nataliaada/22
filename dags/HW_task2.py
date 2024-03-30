from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
import random 

def generate_and_square_number():
    random_number = random.randint(1, 100)
    squared_number = random_number ** 2
    print(f"Random number: {random_number}, Squared number: {squared_number}")

dag = DAG(
    'generate_and_square_number_dag',
    description='Generate and square a random number DAG',
    schedule_interval='0 12 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

generate_random_square = PythonOperator(
    task_id='generate_random_square',
    python_callable=generate_and_square_number,
    dag=dag
)
