from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

def load_files():
    # Здесь код для загрузки файлов boking.csv, client.csv и hotel.csv
    boking_df = pd.read_csv('boking.csv')
    client_df = pd.read_csv('client.csv')
    hotel_df = pd.read_csv('hotel.csv')
    
    return boking_df, client_df, hotel_df

dag = DAG(
    'load_files_dag',
    description='DAG for loading CSV files',
    schedule_interval='@once',
    start_date=datetime(2024, 03, 31),
)

load_files_operator = PythonOperator(
    task_id='load_files_task',
    python_callable=load_files,
    dag=dag,
)
def transform_data(*args, **kwargs):
    boking_df, client_df, hotel_df = kwargs['ti'].xcom_pull(task_ids=['load_files_task'])

    # Объединение таблиц
    merged_df = pd.merge(boking_df, client_df, on='client_id')
    merged_df = pd.merge(merged_df, hotel_df, on='hotel_id')

    # Преобразование дат
    merged_df['date'] = pd.to_datetime(merged_df['date'], format='%Y-%m-%d')

    # Удаление невалидных колонок
    merged_df.drop(columns=['invalid_column'], inplace=True)

    # Приведение валют к одной
    # Например, конвертация в USD

    return merged_df

transform_data_operator = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)
def load_to_database(*args, **kwargs):
    merged_df = kwargs['ti'].xcom_pull(task_ids='transform_data_task')

    # Здесь код для загрузки данных в базу данных

load_to_db_operator = PythonOperator(
    task_id='load_to_db_task',
    python_callable=load_to_database,
    provide_context=True,
    dag=dag,
)

# Определение порядка выполнения операторов
load_files_operator >> transform_data_operator >> load_to_db_operator
