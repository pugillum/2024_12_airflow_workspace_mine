from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="01_rocket_launch",
    start_date=datetime.now() - timedelta(days=90),
    schedule="@daily",
    catchup=True,
) as dag:
    procure_rocket_material = EmptyOperator(task_id="procure_rocket_material")

    building = [EmptyOperator(task_id=f"build_stage_{i}") for i in range(1, 4)]

    procure_fuel = EmptyOperator(task_id="procure_fuel")

    launch_rocket = EmptyOperator(task_id="launch_rocket")

    procure_rocket_material >> building
    procure_fuel >> building[-1]
    building >> launch_rocket
