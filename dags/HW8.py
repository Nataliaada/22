from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from airflow.providers.postgres.operators.postgres import Postgres0perator

def get_data(file_name):
   return pd.read_csv(file_name)

def transform_data(**kwargs):
    booking = kwargs['ti'].xcom pull(task_ids='get_booking')
    client = kwargs['ti'].xcom pull(task_ids='get_client')
    hotel = kwargs['ti'].xcom pull(task_ids='get_hotel')
   
    booking.dropna(inplace=True)
    booking['booking_date'] = booking['booking_date'].str.replace('/)
    hotel.dropna(inplace=True)
    client.dropna(inplace=True)
    client['age'] = client['age'].astype('int')

   data = booking.merge(client, on='client_id').merge(hotel, on='hotel_id')
   data = booking.merge(client, on='client_id').merge(hotel, on='hotel_id')
   data.rename(columns ('name_x': 'name_hotel', 'name_y': 'name_client', 'address': 'hotel_address'), inplace=True)
   data=data[['booking_date', 'client_id', 'name_client', 'age', 'type', 'hotel_id', 'name_hotel', room_type", 'booking_cost', "currency]]

   data.loc[data['currency'] == 'EUR', 'booking_cost'] = (data.loc[data['currency'] == 'EUR booking_cost'] 0.86).round(1) 30 data. loc[data['currency'] == 'EUR', 'currency'] = 'GBP'
   if not os.path.exists('/opt/airflow/dags/data.csv'):
        data.to_csv('/opt/airflow/dags/data.csv', index-False)
   else:
        os.remove('/opt/airflow/dags/data.csv')
        data.to csv('/opt/airflow/dags/data.csv', index=False)

dag DAG( 'data_processing_dag', description = 'DAG for processing and loading data",
  schedule_interval-None, 
        start_date=datetime(2024, 3, 26))
get_booking Python0perator(task_id='get_booking', python_callable_get_data, op_args=['/opt/airflow/dags/booking.csv'1, dag-dag)
get_client = PythonOperator(task_id'get_client', python callable get_data, op args ['/opt/airflow/dags/client.csv'], dag dag)
get_hotel = Python0perator(task_id 'get hotel , python_callable get data, op args=['/opt/airflow/dags/hotel.csv'], dag-dag)
transform_data_task = PythonOperator(task_id 'transform_data_task', python_callable_transform_data, dag=dag)
create_table_postgres = Postgresoperator(task_id = "create_data_table", 
            sql = *** CREATE TABLE IF NOT EXISTS data(
            booking_date DATE,
            client_id INT,
            name_client VARCHAR(50),
            age INT,
            type VARCHAR(20), 
            hotel_id INT, 
            name_hotel VARCHAR(50), 
            room_type VARCHAR(20), 
            booking_cost FLOAT,
            currency VARCHAR(5)); 
            ***,
          postgres_conn_id ='pg_conn',
          database='airflow') 
load_to_postgres_db = PostgresOperator(task_id='load_to_postgres_db',
postgres_conn_id ='pg_conn", 
sql='''COPY data FROM '/opt/airflow/dags/data.csv' WITH CSV HEADER;'''; 
get_booking >> transform_data_task
get_client >> transform_data_task 
get_hotel >> transform_data_task
transform_data_task >> create_table_postgres >> load_to_postgres_db

if__name__=="__mmain__";
dag.cli{}


