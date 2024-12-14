# Hello world

```python
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="hello_world",
    start_date=datetime.now() - timedelta(days=14),
    description="This DAG will print 'Hello' & 'World'.",
    schedule="@daily",
):
    hello = BashOperator(task_id="hello", bash_command="echo 'hello'")
    
    world = PythonOperator(task_id="world", python_callable=lambda: print("world"))

    hello >> world
```

# Print context in Task logs

```python
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from pprint import pprint


def print_context_func(**context):
    pprint(context)


with DAG(dag_id="print_context", schedule=None):
    print_context = PythonOperator(
        task_id="print_context",
        python_callable=print_context_func,
    )
```

# Templating

```python

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_exec_date(**context):
    print(context["execution_date"])


def generate_query(**context):
    sql_query = context["templates_dict"]["sql_query"]

    print("Generated SQL Query:")
    print(sql_query)


with DAG(
    dag_id="templating",
    schedule=None,
):
    print_exec_date_bash = BashOperator(
        task_id="print_exec_date_bash",
        bash_command='echo "{{ execution_date }}"',
    )

    print_exec_date_python = PythonOperator(
        task_id="print_exec_date_python",
        python_callable=print_exec_date,
    )

    sql_query_template = "SELECT * FROM my_table WHERE date_column = '{{ ds }}';"

    generate_query_task = PythonOperator(
        task_id="generate_query_task",
        python_callable=generate_query,
        templates_dict={"sql_query": sql_query_template},
        provide_context=True,
    )

```

# Sensors

```python
# FTP Sensor
from airflow.contrib.sensors.ftp_sensor import FTPSensor

wait_for_data = FTPSensor(
    task_id="wait_for_data",
    path="foobar.json",
    ftp_conn_id="bob_ftp",
)

# Conditional sensor
from datetime import datetime
from airflow.sensors.python import PythonSensor

def _time_for_coffee():
   """I drink coffee between 6 and 12"""
   if 6 < datetime.now().hour < 12:
     return True
   else:
     return False

time_for_coffee = PythonSensor(
   task_id="time_for_coffee",
   python_callable=_time_for_coffee,
   mode="reschedule",
)

```

# Airflow Metadata

```python
# Use of connection
# ...   
    is_api_available = HttpSensor(
        task_id="is_api_available", http_conn_id="thespacedevs_dev", endpoint=""
    )
# ...

# Hooks

from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id="land_registry")

hook.get_records("SELECT * FROM land_registry_price_paid_uk;")

# Variables

from airflow.models import Variable
foo = Variable.get("foo")
bar = Variable.get("bar", deserialize_json=True, default_var=None)

```

# XComs

```python

import random
import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.python import PythonOperator


def _push(task_instance, **_):
    teammembers = ["Bob", "John", "Alice"]
    result = random.choice(teammembers)
    task_instance.xcom_push(key="person_to_email", value=result)
    return result


def _pull(task_instance, **_):
    result_by_key = task_instance.xcom_pull(task_ids="push", key="person_to_email")
    result_by_return_value = task_instance.xcom_pull(task_ids="push")
    print(f"Email {result_by_return_value}")
    print(f"Email {result_by_key}")


with DAG(
    dag_id="example_xcom", start_date=airflow.utils.dates.days_ago(3), schedule="@daily"
):
    push = PythonOperator(task_id="push", python_callable=_push)
    pull = PythonOperator(task_id="pull", python_callable=_pull)
    push >> pull

```

# Branching and joins

```python

from airflow import utils
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator


def _get_weekday(execution_date, **context):
    return execution_date.strftime("%a")  # “Mon”


with DAG(
    dag_id="branch_python_operator_example_complete",
    start_date=utils.dates.days_ago(14),
    schedule="@daily",
) as dag:

    do_something = EmptyOperator(
        task_id="do_something",
    )

    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=_get_weekday,
    )

    do_something >> branching

    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )
    for day in days:
        t = EmptyOperator(
            task_id=day,
        )

        branching >> t >> join

```