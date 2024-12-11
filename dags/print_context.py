from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from pprint import pprint


def print_context_func(**context):
    pprint(context)


with DAG(
    dag_id="print_context",
    start_date=datetime.now() - timedelta(days=14),
    description="This DAG will print 'Hello' & 'World'.",
    schedule="@daily",
):
    hello = BashOperator(task_id="hello", bash_command="echo 'hello'")

    world = PythonOperator(task_id="world", python_callable=lambda: print("world"))

    print_context = PythonOperator(
        task_id="print_context",
        python_callable=print_context_func,
    )

    hello >> world >> print_context
