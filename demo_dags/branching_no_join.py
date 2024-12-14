from airflow import utils
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator


def _get_weekday(execution_date, **context):
    return execution_date.strftime("%a")  # “Mon”


with DAG(
    dag_id="branch_python_operator_example",
    start_date=utils.dates.days_ago(14),
    schedule="@daily",
) as dag:

    do_something = EmptyOperator(
        task_id="do_something",
    )

    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=_get_weekday,
    )

    do_something >> branching

    for day in days:
        branching >> EmptyOperator(
            task_id=day,
        )
