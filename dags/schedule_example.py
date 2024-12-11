from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="schedule_example",
    start_date=datetime(2024, 12, 9),
    description="To help understand the schedule interval",
    schedule="@daily",
):
    hello = BashOperator(task_id="hello", bash_command="echo 'hello'")

    world = PythonOperator(task_id="world", python_callable=lambda: print("world"))

    hello >> world
