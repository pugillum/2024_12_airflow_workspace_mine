import json
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airrlow.providers.google.clolud.transrelrs.bigquery_to_postgres import (
    BigQueryToPostgresOperator,
)

PROJECT_NAME = "aflow-training-bol-2023-06-22"
USER_NAME = "ismaelcv"
LOCAL_STORAGE_PATH = Path("/tmp/launches/")
GOOGLE_CLOUD_CONN_ID = "google_cloud_default"


def _is_launch_today(task_instance):
    launches = json.loads(task_instance.xcom_pull(task_ids="extract_launch"))

    if launches["count"] == 0:
        raise AirflowSkipException(f"No data found on day {context['ds']}")


def _extract_relevant_data(x: dict):
    return {
        "id": x.get("id"),
        "name": x.get("name"),
        "status": x.get("status").get("abbrev"),
        "country_code": x.get("pad").get("country_code"),
        "service_provider_name": x.get("launch_service_provider").get("name"),
        "service_provider_type": x.get("launch_service_provider").get("type"),
    }


def _local_file_storage(task_instance, **context):
    response = task_instance.xcom_pull(task_ids="get_api_result")
    response_dict = json.loads(response)
    response_results = response_dict["results"]
    (
        pd.DataFrame([_extract_relevant_data(i) for i in response_results]).to_parquet(
            path=f"/tmp/{context['ds']}.parquet"
        )
    )


with DAG(
    dag_id="04_capstone_project",
    start_date=datetime(2023, 6, 10),
    schedule="@daily",
    catchup=True,
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
        python_callable=_is_launch_today,
    )

    local_file_storage = PythonOperator(
        task_id="local_file_storage",
        python_callable=_local_file_storage,
    )

    cloud_file_storage = LocalFilesystemToGCSOperator(
        gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
        task_id="cloud_file_storage",
        src="/tmp/launches/{{ds}}.parquet",
        dst=f"{USER_NAME}" + "/launches/{{ds}}.parquet",
        bucket=PROJECT_NAME,
    )

    create_new_BQ_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="new_dataset_creator",
        dataset_id=f"{USER_NAME}_dataset",
        project_id=PROJECT_NAME,
        gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=PROJECT_NAME,
        source_objects=[f"{USER_NAME}" + "/launches/{{ds}}.parquet"],
        destination_project_dataset_table=f"{PROJECT_NAME}.{USER_NAME}_dataset.rocket_launches",
        source_format="parquet",
        write_disposition="WRITE_APPEND",
        autodetect=True,
        gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
    )

    create_postgres_table = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS rocket_launches (
                id TEXT NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                country TEXT NOT NULL,
                launch_service_provider_name TEXT NOT NULL,
                launch_service_provider_type TEXT 
            );
        """,
    )

    """
    Remember! To see the data in postgres db:
        docker ps
        docker exec -it <db-postgres name> /bin/bash
        psql -Uairflow
        SELECT * FROM rocket_launches;
    """

    store_launch_in_postgres_db = BigQueryToPostgresOperator(
        task_id="insert_into_postgres_table",
        dataset_table="steve.rocket_launches${{ ds_nodash }}",
        target_table_name="rocket_launches",
        postgres_conn_id="postgres",
    )

    (is_api_available >> extract_launch >> is_there_launch_today >> local_file_storage)

    (
        local_file_storage
        >> cloud_file_storage
        >> create_new_BQ_dataset
        >> load_to_bigquery
    )

    (local_file_storage >> create_postgres_table >> store_launch_in_postgres_db)
