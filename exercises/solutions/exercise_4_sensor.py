from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor


# https://httpstat.us/random/200,400


def _process_data():
    print("Processing data...")


with DAG(
    dag_id="exercise_4",
    schedule=None,
):
    is_api_available = HttpSensor(
        task_id="is_api_available", http_conn_id="http_check", endpoint=""
    )

    process_data = PythonOperator(
        task_id="process_data",
        python_callable=_process_data,
    )

    is_api_available >> process_data
