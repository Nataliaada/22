from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hello(name):
    return f'Hello {name}'
your_name = 'Natalia'    

dag = DAG('my_first_dag', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello,op_kwargs={'name': your_name} dag=dag)

hello_operator
