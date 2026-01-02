import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import cross_downstream
from airflow.sdk import chain

with DAG("veera_ex2", start_date=datetime.datetime(2021, 1, 1), schedule="@daily") as dag:
    first = EmptyOperator(task_id="first")
    last = EmptyOperator(task_id="last")

    options = ["branch_a", "branch_b", "branch_c", "branch_d"]
    for option in options:
        t = EmptyOperator(task_id=option)
        first >> t >> last