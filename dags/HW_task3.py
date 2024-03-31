from datetime import datetime
from airflow import DAG
from airflow.operators import BashOperator,PythonOperator
def print_hello ():
    return 'Hello world from Airflow DAG!'
def skipp():
    return 99
dag = DAG( 'link' , description= 'Your are here DAG' ,
schedule_interval= '0 12 * * *' ,
start_date=datetime( 2023 , 1 , 1
), catchup= False )


hello_file_operator = PythonOperator(task_id= 'hello_file_task' , 
                                     bash_command= 'https://goweather.herokuapp.com/weather/"location', 
                                     dag=dag)
hello_operator >> hello_file_operator

from datetime import datetime
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator

# Замените 'your_location' на ваше реальное местоположение
location = 'moscow'

dag = DAG(
    'weather_api_dag',
    description='Send HTTP request to get weather data for a specific location',
    schedule_interval='0 12 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

get_weather_operator = SimpleHttpOperator(
    task_id='get_weather_data',
    http_conn_id='http_default',  # Используйте ваше соединение HTTP, если необходимо
    method='GET',
    endpoint=f'/weather/{location}',
    headers={"Content-Type": "application/json"},
    xcom_push=True,
    dag=dag
)
