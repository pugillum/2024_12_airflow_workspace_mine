from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dataset_long_dag",
    start_date=datetime(year=2019, month=1, day=1),
    schedule=None,
    catchup=False,
):
    get_cleaned_data = BashOperator(
        task_id="get_cleaned_data", bash_command="echo 'get_cleaned_data'"
    )

    print_date_time = PythonOperator(
        task_id="print_date_time", python_callable=lambda: print(datetime.now())
    )

    remove_outliers = BashOperator(
        task_id="remove_outliers", bash_command="echo 'remove_outliers'"
    )

    tag_images = PythonOperator(
        task_id="tag_images", python_callable=lambda: print("tag_images")
    )

    # produce report should intentionally fail
    produce_report = BashOperator(task_id="produce_report", bash_command="exit 1")

    (
        get_cleaned_data
        >> [print_date_time, remove_outliers]
        >> tag_images
        >> produce_report
    )
