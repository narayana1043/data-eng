"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/main/airflow/example_dags/tutorial.py
"""

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

import datetime
import pendulum

dag = DAG(
    "veera_ex4",
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=3),
    },
    start_date=pendulum.datetime(2015, 12, 1, tz="UTC"),
    description="A simple tutorial Dag",
    schedule="@daily",
)