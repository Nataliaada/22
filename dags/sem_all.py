from datetime import datetime
import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

sem8dag = DAG( 'sem8dag', description= 'sem8dag',
schedule_interval= '0 12 * * *' ,
start_date=datetime( 2021,4,1),
catchup= False )

def get_weather_data(ti = None):
url = "https://api.openweathermap.org/data/2.5/weather?q=London,uk&APPID=cde25583263075ff71ffa3961bd239d9"
response = requests.request("GET", url)

temperature_openweather = json.loads(response.text)['main']['temp'] - 273
ti.xcom_push(key="temperature_openweather", value=temperature_openweather)

headers = {
'X-Yandex-Weather-Key': "5366727e-2841-4d55-92d9-281216c4493d"
}

response = requests.get('https://api.weather.yandex.ru/v2/forecast?lat=51.5085&lon=0.12574', headers=headers)

temperature_yandex = response.json()['fact']['temp']

ti.xcom_push(key="temperature_yandex", value=temperature_yandex)


get_weather = PythonOperator(task_id = "get_weather_data",python_callable=get_weather_data,dag = sem8dag)

send_tg_message = TelegramOperator(
task_id='send_telega',
token="7191996568:AAHo61BtPMh1U5Ldyv8DxTGv2KOTLdaH9tU",
chat_id=1211274716,
text='Температура в Лондоне по версии Openweather: \
{{ ti.xcom_pull(key="temperature_openweather", task_ids="get_weather_data") }}\n \
Температура в Лондоне по версии Yandex: \
{{ ti.xcom_pull(key="temperature_yandex", task_ids="get_weather_data") }}',
dag=sem8dag
)

get_weather >> send_tg_message
