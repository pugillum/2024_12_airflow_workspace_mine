from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import DAG
import airflow.utils.dates
import requests

API_URL = "https://lldev.thespacedevs.com/2.2.0/launch"

with DAG(
    dag_id="exercise_5_xcoms",
    start_date=airflow.utils.dates.days_ago(7),
    schedule_interval="@daily",
    tags=["exercise"],
) as dag:

    def _download_launches(task_instance, **context):
        templates_dict = context["templates_dict"]

        response = requests.get(
            API_URL,
            params={
                "window_start__gte": templates_dict["window_start"],
                "window_end__lt": templates_dict["window_end"],
            },
        )
        response.raise_for_status()
        return response.json()

    def _print_launch_count(task_instance, **_):

        launches = task_instance.xcom_pull(task_ids="download_launches")

        # input_path = context["templates_dict"]["input_path"]

        # with Path(input_path).open() as file_:
        #     launches = json.load(file_)

        print(f"""Counted {launches["count"]} launches""")

    print_date = BashOperator(
        task_id="print_date", bash_command="echo {{ execution_date }}"
    )

    download_launches = PythonOperator(
        task_id="download_launches",
        python_callable=_download_launches,
        templates_dict={
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
