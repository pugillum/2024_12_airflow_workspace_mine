from datetime import datetime
import json
import os

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator


def is_launch_today(task_instance, **context):
    response = task_instance.xcom_pull(task_ids="extract_launch")
    response_dict = json.loads(response)
    if response_dict["count"] == 0:
        raise AirflowSkipException(f"No data found on day {context['ds']}")


def convert_to_parquet(task_instance, **context):
    response = task_instance.xcom_pull(task_ids="extract_launch")
    response_dict = json.loads(response)
    response_results = response_dict["results"]

    with open(f"/tmp/{context['ds']}.json", "w") as f:
        json.dump(response_results, f)

    print(f"Data saved to /tmp/{context['ds']}.json")
    # show files in /tmp
    print("Files in tmp:", os.listdir("/tmp"))


with DAG(
    dag_id="capstone_project_get_api_data",
    start_date=datetime(2023, 12, 11),
    end_date=datetime(2023, 12, 16),
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

    is_there_launch_today = PythonOperator(
        task_id="is_there_launch_today",
        python_callable=is_launch_today,
    )

    convert_to_parquet = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=convert_to_parquet,
    )

    cloud_file_storage = EmptyOperator(task_id="cloud_file_storage")

    create_new_BQ_dataset = EmptyOperator(task_id="new_dataset_creator")

    load_to_bigquery = EmptyOperator(task_id="load_to_bigquery")

    create_postgres_table = EmptyOperator(task_id="create_postgres_table")

    store_launch_in_postgres_db = EmptyOperator(task_id="store_launch_in_postgres_db")

    is_api_available >> extract_launch >> is_there_launch_today >> convert_to_parquet

    (
        convert_to_parquet
        >> cloud_file_storage
        >> create_new_BQ_dataset
        >> load_to_bigquery
        >> convert_to_parquet
        >> create_postgres_table
        >> store_launch_in_postgres_db
    )
