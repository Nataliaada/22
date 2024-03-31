import datetime
import os
import requests
import pendulum
from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator

os.environ["no_proxy"] = "*"

@dag(
    dag_id="weather-telegram",
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)
def WeatherETL():

    send_message_telegram_task = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='tg_main',
        token= '7191996568:AAHo61BtPMh1U5Ldyv8DxTGv2KOTLdaH9tU'
        chat_id='-1211274716',
        text='Weather in Moscow \nYandex: ' + "{{ ti.xcom_pull(task_ids=['yandex_weather'], key='weather')[0]}}" + " degrees" +
             "\nOpen weather: " + "{{ ti.xcom_pull(task_ids=['open_weather'], key='weather')[0]}}" + " degrees",
    )
https://api.telegram.org/bot7191996568:AAHo61BtPMh1U5Ldyv8DxTGv2KOTLdaH9tU/getUpdates
    @task(task_id='yandex_weather')
    def get_yandex_weather(**kwargs):
        ti = kwargs['ti']
        url = "https://api.weather.yandex.ru/v2/informers/?lat=55.75396&lon=37.620393"
        payload = {}
        headers = {
            'X-Yandex-API-Key': '33f45b91-bcd4-46e4-adc2-33cfdbbdd88e'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        ti.xcom_push(key='weather', value=response.json()['fact']['temp'])

    @task(task_id='open_weather')
    def get_open_weather(**kwargs):
        ti = kwargs['ti']
        url = ("https://api.openweathermap.org/data/2.5/weather?lat=55.749013596652574&lon=37.61622153253021&"
               "appid=2cd78e55c423fc81cebc1487134a6300")
        payload = {}
        headers = {}
        response = requests.request("GET", url, headers=headers, data=payload)
        ti.xcom_push(key='weather', value=round(float(response.json()['main']['temp']) - 273.15, 2))

    get_yandex_weather() >> get_open_weather() >> send_message_telegram_task

dag = WeatherETL()
