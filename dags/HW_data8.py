from datetime import datetime
import pandas as pd
import os
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    'data1_processing_dag',
    default_args=default_args,
    description='DAG for processing and loading data',
    schedule_interval=None
)

def get_data(file_name):
    return pd.read_csv(file_name)

def transform_data(**kwargs):
    booking = kwargs['ti'].xcom_pull(task_ids='get_booking')
    client = kwargs['ti'].xcom_pull(task_ids='get_client')
    hotel = kwargs['ti'].xcom_pull(task_ids='get_hotel')

    # Transformation steps
    # Merge all tables
    data = booking.merge(client, on='client_id').merge(hotel, on='hotel_id')
    
    # Data cleaning and transformation
    data['booking_date'] = pd.to_datetime(data['booking_date'], format='%Y-%m-%d')  # Assuming booking date format is YYYY-MM-DD
    data['currency'] = 'USD'  # Convert all currencies to USD
    
    relevant_columns = ['booking_date', 'client_id', 'name_client', 'age', 'type', 'hotel_id', 'name_hotel', 'room_type', 'booking_cost', 'currency']
    data = data[relevant_columns]

    # Saving transformed data to a new CSV file
    output_file = 'https://github.com/Nataliaada/22/blob/main/dags/data_transformed.csv'
    data.to_csv(output_file, index=False)

    return output_file

def load_to_postgres(**kwargs):
    file_path = kwargs['ti'].xcom_pull(task_ids='transform_data_task')

    with open(file_path, 'r') as f:
        sql_query = f"COPY data FROM '{file_path}' WITH CSV HEADER"
        # Execute SQL query to load data into Postgres
        # Example: psycopg2.connect('database').execute(sql_query)

# Task to get data from CSV files
get_booking = PythonOperator(
    task_id='get_booking',
    python_callable=get_data,
    op_args=['https://github.com/Nataliaada/22/blob/main/dags/booking.csv'],
    dag=dag
)

get_client = PythonOperator(
    task_id='get_client',
    python_callable=get_data,
    op_args=['https://github.com/Nataliaada/22/blob/main/dags/client.csv'],
    dag=dag
)

get_hotel = PythonOperator(
    task_id='get_hotel',
    python_callable=get_data,
    op_args=['https://github.com/Nataliaada/22/blob/main/dags/hotel.csv'],
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_to_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag
)

get_booking >> transform_data_task
get_client >> transform_data_task
get_hotel >> transform_data_task
transform_data_task >> load_to_postgres_task
