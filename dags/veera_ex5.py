import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

dag = DAG(
    "veera_ex5",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)

parameterized_task = BashOperator(
    task_id="parameterized_task",
    bash_command="echo value: {{ dag_run.conf['conf1'] }}",
    dag=dag,
)
