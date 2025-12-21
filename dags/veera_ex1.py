import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import cross_downstream
from airflow.sdk import chain



my_dag = DAG(
    dag_id="veera_ex1",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
    access_control={
        "veera_role": {"can_edit", "can_read", "can_delete"},
    },
)

tasks = {}
for i in range(1, 6):
    tasks[i] = EmptyOperator(task_id=f"task{i}", dag=my_dag)

op = {}
for i in range(1, 7):
    op[i] = EmptyOperator(task_id=f"op{i}", dag=my_dag)

cross_downstream([tasks[1], tasks[2]], [tasks[3], tasks[4], tasks[5]])

chain(*[EmptyOperator(task_id=f"oper{i}", dag=my_dag) for i in range(1, 6)])

chain(op[1], [op[2], op[3]], [op[4], op[5]], op[6])