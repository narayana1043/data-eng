import datetime

from airflow.sdk import DAG
from airflow.sdk import task_group
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="veera_ex3",
    start_date=datetime.datetime(2016, 1, 1),
    schedule="@daily",
    default_args={"retries": 1},
):

    @task_group(default_args={"retries": 3})
    def group1():
        """This docstring will become the tooltip for the TaskGroup."""
        task1 = EmptyOperator(task_id="task1")
        task2 = BashOperator(task_id="task2", bash_command="echo Hello World!", retries=2)
        print(task1.retries)  # 3
        print(task2.retries)  # 2

    task3 = EmptyOperator(task_id="task3")
    group1() >> task3