from airflow.models import DAG
from airflow.operators.python import PythonOperator

from pprint import pprint


def print_context_func(**context):
    pprint(context)


with DAG(dag_id="print_context", schedule=None, is_paused_upon_creation=False):
    print_context = PythonOperator(
        task_id="print_context",
        python_callable=print_context_func,
    )
