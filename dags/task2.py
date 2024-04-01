from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


dag = DAG( 'hello_world' , description= 'Hello World DAG' ,
schedule_interval= '0 12 * * *' ,
start_date=datetime( 2023 , 1 , 1
), catchup= False )

hello_operator = BashOperator(task_id= 'hello_task' , bash_command='echo Hello from Airflow', dag=dag)
skipp_operator = BashOperator(task_id= 'skip_task' , bash_command='exit 99', dag=dag)
hello_file_operator = BashOperator(task_id= 'hello_file_task' ,
bash_command='./home/airflow/airflow/dags/scripts/file1.sh', dag=dag)
hello_operator >> skipp_operator >> hello_file_operator

def print_hello ():
    return 'Hello world from Airflow DAG!'
def skipp():
    return 99
dag2 = DAG( 'hello_world2' , description= 'Hello World DAG' ,
schedule_interval= '0 12 * * *' ,
start_date=datetime( 2023 , 1 , 1
), catchup= False 
)
hello_operator2 = PythonOperator(task_id= 'hello_task2' , python_callable=print_hello, dag=dag2)
skipp_operator2 = PythonOperator(task_id= 'skip_task2' , python_callable=skipp, dag=dag2)
hello_file_operator2 = BashOperator(task_id= 'hello_file_task2' , bash_command='/home/airflow/airflow/dags/scripts/file1.py', dag=dag2)
hello_operator2 >> skipp_operator2 >> hello_file_operator2
