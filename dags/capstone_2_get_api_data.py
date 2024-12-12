from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator


with DAG(
    dag_id="capstone_project_get_api_data",
    start_date=datetime(2023, 12, 11),
    end_date=datetime(2023, 12, 25),
    schedule="@daily",
):
    is_api_available = HttpSensor(
        task_id="is_api_available", http_conn_id="thespacedevs_dev", endpoint=""
    )

    extract_launch = SimpleHttpOperator(
        task_id="extract_launch",
        http_conn_id="thespacedevs_dev",
        endpoint="",
        method="GET",
        data={
            "window_start__gte": "{{ds}}T00:00:00Z",
            "window_end__lt": "{{next_ds}}T00:00:00Z",
        },
        log_response=True,
    )

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
