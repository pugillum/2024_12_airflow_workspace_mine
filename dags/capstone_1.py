from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="capstone_project",
    start_date=datetime(2024, 12, 11),
    schedule="@daily",
):
    is_api_available = EmptyOperator(task_id="is_api_available")

    extract_launch = EmptyOperator(task_id="extract_launch")

    is_there_launch_today = EmptyOperator(task_id="is_there_launch_today")

    local_file_storage = EmptyOperator(task_id="local_file_storage")

    cloud_file_storage = EmptyOperator(task_id="cloud_file_storage")

    create_new_BQ_dataset = EmptyOperator(task_id="new_dataset_creator")

    load_to_bigquery = EmptyOperator(task_id="load_to_bigquery")

    create_postgres_table = EmptyOperator(task_id="create_postgres_table")

    store_launch_in_postgres_db = EmptyOperator(task_id="store_launch_in_postgres_db")

    is_api_available >> extract_launch >> is_there_launch_today >> local_file_storage

    (
        local_file_storage
        >> cloud_file_storage
        >> create_new_BQ_dataset
        >> load_to_bigquery
    )

    local_file_storage >> create_postgres_table >> store_launch_in_postgres_db
