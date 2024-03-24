from datetime import datetime
from airflow import DAG
from airflow.operators import BashOperator,PythonOperator

dag = DAG( 'hello_world' , description= 'Hello World DAG' ,
schedule_interval= '0 12 * * *' ,
start_date=datetime( 2023 , 1 , 1
), catchup= False )

hello_operator = BashOperator(task_id= 'hello_task' , bash_command='echo Hello from Airflow', dag=dag)
skipp_operator = BashOperator(task_id= 'skip_task' , bash_command='exit 99', dag=dag)
hello_file_operator = BashOperator(task_id= 'hello_file_task' ,
bash_command='./home/airflow/airflow/dags/scripts/file1.sh', dag=dag)
hello_operator >> skipp_operator >> hello_file_operator