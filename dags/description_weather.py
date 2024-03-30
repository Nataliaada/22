from datetime import datetime
from airflow import DAG
from airflow.operators.bash operator import BashOperator 
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.http_operator import SimpleHttpOperator 
import random 
import json

default args = {
'owner': 'airflow',
'start_date': datetime(2024, 3, 11), 
'retries': 1 
} 

def random_square_print():
num = random.randint(1, 100)
res = num ** 2
print(f"Original number = {num}, Squared number= {res}.")


def print weather(**kwargs): 
response = kwargs['ti'].xcom_pull(key=None, task_ids='get_weather') 
data = json. loads(response)
print(f"Weather in Sankt-Petersburg: temperature {data['temperature']}; wind {data['wind']}; description {data['description']}.")
                                                                              
dag = DAG(dag_id='get_weather', default_args=default_args, schedule_interval=None)
 taskl = BashOperator(
 task_id ='print_random num bash',
 bash_command= 'echo $((RANDOM % 100))', 
 dag=dag
)

task2 = PythonOperator( 
 task_id ='print_random_square_num', 
 python_callable=random_square_print,
 dag=dag
)

task3 = SimpleHttpOperator( 
 task_id='get weather', 
 method='GET',
 http_conn_id='goweather_api',
 endpoint='/weather/Sankt-Petersburg', 
 headers = (),
 dag=dag
)

task4 = PythonOperator( 
 task id = 'print weather'
 python callable=print weather,
 provide context=True,
 dag=dag
)
task1 >> task2 >> task3 >> task4

