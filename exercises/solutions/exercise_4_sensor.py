from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor

with DAG(
    dag_id="exercise_4",
    schedule=None,
    is_paused_upon_creation=False,
    tags=["exercise"],
):
    wait_for_api = HttpSensor(
        task_id="wait_for_api",
        http_conn_id="http_delayed",
        endpoint="",
    )

    do_something = BashOperator(
        task_id="do_something",
        bash_command="echo 'doing something'",
    )

    wait_for_api >> do_something
