import json
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import DAG
import airflow.utils.dates
import requests

API_URL = "https://lldev.thespacedevs.com/2.3.0/launch"


default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="exercise_templating",
    start_date=airflow.utils.dates.days_ago(7),
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    def _download_launches(**context):
        templates_dict = context["templates_dict"]
        output_path = Path(templates_dict["output_path"])

        response = requests.get(
            API_URL,
            params={
                "window_start__gte": templates_dict["window_start"],
                "window_end__lt": templates_dict["window_end"],
            },
        )
        response.raise_for_status()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w") as file_:
            json.dump(response.json(), file_)

    def _print_launch_count(**context):
        input_path = context["templates_dict"]["input_path"]

        with Path(input_path).open() as file_:
            launches = json.load(file_)

        print(f"""Counted {launches["count"]} launches from {input_path}""")

    print_date = BashOperator(
        task_id="print_date", bash_command="echo {{ logical_date }}"
    )

    download_launches = PythonOperator(
        task_id="download_launches",
        python_callable=_download_launches,
        templates_dict={
            "output_path": "/tmp/launches/{{ds}}.json",
            "window_start": "{{ds}}T00:00:00Z",
            "window_end": "{{next_ds}}T00:00:00Z",
        },
    )

    check_for_launches = PythonOperator(
        task_id="check_for_launches",
        python_callable=_print_launch_count,
        templates_dict={"input_path": "/tmp/launches/{{ds}}.json"},
    )

    print_date >> download_launches >> check_for_launches
