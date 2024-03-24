task_http_sensor_check = HttpSensor(
task_id="http_sensor_check",
http_conn_id="http_default",
endpoint="",
request_params={},
response_check=lambda response: "httpbin" in response.text,
poke_interval=5,
dag=dag,
)
hello_operator >> skipp_operator >> hello_file_operator >> task_http_sensor_check