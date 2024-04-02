from datetime import datetime
import pandas as pd
import os
from airflow.models import DAG
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

def get_data(file_name):
    return pd.read_csv(file_name)

def transform_data(**kwargs):
    # Ваша функция transform_data без изменений

dag = DAG('homework8',
          description='DAG for processing and loading data',
          schedule_interval=None,
          start_date=datetime(2024, 3, 26))

get_booking = PythonOperator(task_id='get_booking', python_callable=get_data,
                            op_args=['/opt/airflow/dags/booking.csv'], dag=dag)
get_client = PythonOperator(task_id='get_client', python_callable=get_data,
                           op_args=['/opt/airflow/dags/client.csv'], dag=dag)
get_hotel = PythonOperator(task_id='get_hotel', python_callable=get_data,
                          op_args=['/opt/airflow/dags/hotel.csv'], dag=dag)

transform_data_task = PythonOperator(task_id='transform_data_task',
                                     python_callable=transform_data, dag=dag)

create_table_postgres = PostgresOperator(task_id="create_data_table",
                                         sql="""CREATE TABLE IF NOT EXISTS data(
             booking_date DATE,
             client_id INT,
             name_client VARCHAR(50),
             age INT,
             type VARCHAR(20),
             hotel_id INT,
             name_hotel VARCHAR(50),
             room_type VARCHAR(20),
             booking_cost FLOAT,
             currency VARCHAR(5));""",
                                         postgres_conn_id='pg_conn',
                                         database='airflow')

load_to_postgres_db = PostgresOperator(task_id='load_to_postgres_db',
                                       postgres_conn_id='pg_conn',
                                       sql="""COPY data FROM '/opt/airflow/dags/data.csv' WITH CSV HEADER;""",
                                       dag=dag)

get_booking >> transform_data_task
get_client >> transform_data_task
get_hotel >> transform_data_task
transform_data_task >> create_table_postgres >> load_to_postgres_db

globals()['dag'] = dag

