from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="02_rocket_launch_3days",
    start_date=datetime.now() - timedelta(days=90),
    schedule=timedelta(days=3),
    catchup=True,
) as dag:
    procure_rocket_material = EmptyOperator(task_id="procure_rocket_material")

    building = [EmptyOperator(task_id=f"build_stage_{i}") for i in range(1, 4)]

    procure_fuel = EmptyOperator(task_id="procure_fuel")

    launch_rocket = EmptyOperator(task_id="launch_rocket")

    procure_rocket_material >> building
    procure_fuel >> building[-1]
    building >> launch_rocket

with DAG(
    dag_id="02_rocket_launch_after_lunch_mon_wed_fri",
    start_date=datetime.now() - timedelta(days=90),
    schedule="45 13 * * 1,3,5",
    catchup=True,
) as dag:
    procure_rocket_material = EmptyOperator(task_id="procure_rocket_material")

    building = [EmptyOperator(task_id=f"build_stage_{i}") for i in range(1, 4)]

    procure_fuel = EmptyOperator(task_id="procure_fuel")

    launch_rocket = EmptyOperator(task_id="launch_rocket")

    procure_rocket_material >> building
    procure_fuel >> building[-1]
    building >> launch_rocket
