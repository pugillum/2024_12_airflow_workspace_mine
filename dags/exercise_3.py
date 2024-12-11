from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_message(**context):
    # print(f"This DAG run was for period {context["data_interval_start"]} to {context["data_interval_end"]}")
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]
    print(
        f"🔥 This DAG run was for period {data_interval_start} to {data_interval_end}"
    )
    print(f"🔥 This DAG run was triggered on {context['dag_run'].queued_at}")


with DAG(
    dag_id="exercise_3",
    description="This DAG will print 'Hello' & 'World'.",
    start_date=datetime.now() - timedelta(days=2),
    schedule="@daily",
):
    echo_task = BashOperator(
        task_id="echo_task",
        bash_command='echo "🔥 Task ID: {{ task.task_id }}, DAG ID: {{ dag.dag_id }}"',
    )

    date_task = PythonOperator(
        task_id="date_task",
        python_callable=print_message,
    )

    echo_task >> date_task
