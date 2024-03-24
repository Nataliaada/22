from datetime import datetime
from airflow import DAG
from airflow.operators import BashOperator,PythonOperator
def print_hello ():
return 'Hello world from Airflow DAG!'
def skipp():
return 99
dag = DAG( 'hello_world' , description= 'Hello World DAG' ,
schedule_interval= '0 12 * * *' ,
start_date=datetime( 2023 , 1 , 1
), catchup= False )
hello_operator = PythonOperator(task_id= 'hello_task' , python_callable=print_hello, dag=dag)
skipp_operator = PythonOperator(task_id= 'skip_task' , python_callable=skipp, dag=dag)
hello_file_operator = BashOperator(task_id= 'hello_file_task' , bash_command='python
/home/airflow/airflow/dags/scripts/file1.py', dag=dag)
hello_operator >> skipp_operator >> hello_file_operator