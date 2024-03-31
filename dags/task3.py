from datetime import datetime
from airflow import DAG
from airflow.operators import BashOperator,PythonOperator
def print_hello ():
    return 'Hello world from Airflow DAG!'
def skipp():
    return 99

dag = DAG( 'hello_world3' , description= 'Hello World DAG' ,
schedule_interval= '0 12 * * *' ,
start_date=datetime( 2023 , 1 , 1
), catchup= False )

hello_operator = PythonOperator(task_id= 'hello_task' , python_callable=print_hello, dag=dag)
skipp_operator = PythonOperator(task_id= 'skip_task' , python_callable=skipp, dag=dag)
hello_file_operator = BashOperator(task_id= 'hello_file_task' , bash_command='python/home/airflow/airflow/dags/scripts/file1.py', dag=dag)


task_http_sensor_check = HttpSensor(
task_id="http_sensor_check",
http_conn_id="http_default",
endpoint="",
request_params={},
response_check=lambda response: "httpbin" in response.text,
poke_interval=5,
dag=dag,
)


hello_operator >> skipp_operator >> hello_file_operator >> task_http_sensor_check
