from datetime import datetime
from airflow import DAG
from airflow.operators import BashOperator,PythonOperator
def print_hello ():
    return 'Hello world from Airflow DAG!'
def skipp():
    return 99

dag3 = DAG( 'hello_world3' , description= 'Hello World DAG' ,
schedule_interval= '0 12 * * *' ,
start_date=datetime( 2023 , 1 , 1
), catchup= False )

hello_operator = PythonOperator(task_id= 'hello_task' , python_callable=print_hello, dag=dag3)
skipp_operator = PythonOperator(task_id= 'skip_task' , python_callable=skipp, dag=dag3)
hello_file_operator = BashOperator(task_id= 'hello_file_task' , bash_command='/home/airflow/airflow/dags/scripts/file1.py', dag=dag3)


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
