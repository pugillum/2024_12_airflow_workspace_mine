from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# using a decorator
@dag(
    dag_id="hello_world_3",
    start_date=datetime.now() - timedelta(days=14),
    description="This DAG will print 'Hello' & 'World'.",
    schedule="@daily",
)
def generate_dag():
    hello = BashOperator(task_id="hello", 
                         bash_command="echo 'hello'")

    world = PythonOperator(task_id="world", 
                           python_callable=lambda: print("world"))

    hello >> world
