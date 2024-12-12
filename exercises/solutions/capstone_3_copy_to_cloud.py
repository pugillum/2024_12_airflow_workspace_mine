from datetime import datetime
import json
import os

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

import pandas as pd

GOOGLE_CLOUD_CONN_ID = "google_cloud_default"
USER_NAME = "travis"
PROJECT_NAME = "airflow-training-20241210"


def is_launch_today(task_instance, **context):
    response = task_instance.xcom_pull(task_ids="extract_launch")
    response_dict = json.loads(response)
    if response_dict["count"] == 0:
        raise AirflowSkipException(f"No data found on day {context['ds']}")


def extract_relevant_data(x: dict):
    return {
        "id": x.get("id"),
        "name": x.get("name"),
        "status": x.get("status").get("abbrev"),
        "country_code": x.get("pad").get("country_code"),
        "service_provider_name": x.get("launch_service_provider").get("name"),
        "service_provider_type": x.get("launch_service_provider").get("type"),
    }


def convert_to_parquet(task_instance, **context):
    response = task_instance.xcom_pull(task_ids="extract_launch")
    response_dict = json.loads(response)
    response_results = response_dict["results"]

    # create /tmp/lauches directory if it does not exist
    os.makedirs("/tmp/launches", exist_ok=True)

    (
        pd.DataFrame([extract_relevant_data(i) for i in response_results]).to_parquet(
            path=f"/tmp/launches/{context['ds']}.parquet"
        )
    )

    print(f"Writen to /tmp/launches/{context['ds']}.parquet")
    print("Files in launches:", os.listdir("/tmp/launches"))


with DAG(
    dag_id="capstone_project_copy_to_cloud",
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

    cloud_file_storage = LocalFilesystemToGCSOperator(
        gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
        task_id="cloud_file_storage",
        src="/tmp/launches/{{ds}}.parquet",
        dst=f"{USER_NAME}" + "/launches/{{ds}}.parquet",
        bucket=PROJECT_NAME,
    )

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
    )

    convert_to_parquet >> create_postgres_table >> store_launch_in_postgres_db
