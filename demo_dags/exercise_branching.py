import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

dag = DAG(
    dag_id="exercise_branching",
    default_args={
        "owner": "godatadriven",
        "start_date": airflow.utils.dates.days_ago(14),
    },
    description="DAG demonstrating BranchPythonOperator.",
    schedule_interval="0 0 * * *",
)


def print_weekday(execution_date, **_):
    print(execution_date.strftime("%a"))


print_weekday = PythonOperator(
    task_id="print_weekday", python_callable=print_weekday, dag=dag
)

# Definition of who is emailed on which weekday
weekday_person_to_email = {
    0: "Bob",  # Monday
    1: "Joe",  # Tuesday
    2: "Alice",  # Wednesday
    3: "Joe",  # Thursday
    4: "Alice",  # Friday
    5: "Alice",  # Saturday
    6: "Alice",  # Sunday
}


# Function returning name of task to execute
def _get_person_to_email(execution_date, **_):
    person = weekday_person_to_email[execution_date.weekday()]
    return "email_{person}".format(person=person.lower())


# Branching task, the function above is passed to python_callable​
branching = BranchPythonOperator(
    task_id="branching", python_callable=_get_person_to_email, dag=dag
)


print_weekday >> branching

final_task = BashOperator(
    task_id="final_task",
    trigger_rule=TriggerRule.ONE_SUCCESS,
    bash_command="sleep 5",
    dag=dag,
)

# Create dummy tasks for names in the dict, and execute all after the branching task
for name in set(weekday_person_to_email.values()):
    email_task = DummyOperator(
        task_id="email_{name}".format(name=name.lower()), dag=dag
    )
    branching >> email_task >> final_task
