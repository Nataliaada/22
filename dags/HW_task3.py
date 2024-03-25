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


hello_file_operator = PythonOperator(task_id= 'hello_file_task' , bash_command= 'https://goweather.herokuapp.com/weather/"location', dag=dag)
hello_operator >> hello_file_operator
