from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_exec_date(**context):
    print(context["execution_date"])


def generate_query(**kwargs):
    sql_query = kwargs["templates_dict"]["sql_query"]

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
