from datetime import datetime
from airflow import DAG
from airflow.operators.bash operator import Bash0perator
from airflow.operators.python operator import Python0perator
from airflow.operators.http operator import SimpleHttp0perator
import random
import json
default_args =
'owner': 'airflow',
'start date : datetime(2024, 3, 11),
'retries' : 1

def random square print(): 16 num = random.randint(1, 100)
    res = num**
    print(f"Original number = {num}. Squared number = {res}.")
def print weather(**kwargs):
    response = kwargs['ti'].xcom_pull(key=None, task_ids='get_weather')
    data = json. loads(response)
def print weather(**kwargs):
    response = kwargs['ti'].xcom pull(key=None, task_ids='get weather') data = json. loads(response)
    print(f"Weather in Sankt-Petersburg: temperature {data['temperature']}; wind {data['wind
dag = DAG(dag id='get weather', default args=defaultargs, schedule interval=None)
task1 = Bash0perator(
task_id ='print_random num bash',
bash_command = 'echo $((RANDOM °, 100))
dag=dag
task2 = Python0perator(
task_id = 'print_random_square_num',
python_callable=random_square_print,
dag=dag
task3 = SimpleHttpOperator(
task id='get weather'
method='GET',
http_conn_id='goweather_api',
endpoint='/weather/Sankt-Petersburg',
headers =
dag=dag
task4 = Python0perator(
task id = 'print weather',
python_callable=print_weather,
provide context=True,
dag=dag
)
task1 >> task2 >> task3 >> task4
