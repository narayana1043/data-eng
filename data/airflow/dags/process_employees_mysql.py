import datetime
import pendulum
import os

import requests
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    dag_id="process_employees_mysql",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessEmployees():
    create_employees_table = SQLExecuteQueryOperator(
        task_id="create_employees_table",
        conn_id="tutorial_mysql_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS employees (
                `Serial Number` INT PRIMARY KEY,
                `Company Name` VARCHAR(255),
                `Employee Markme` VARCHAR(255),
                `Description` TEXT,
                `Leave` INT
            );""",
    )

    create_employees_temp_table = SQLExecuteQueryOperator(
        task_id="create_employees_temp_table",
        conn_id="tutorial_mysql_conn",
        sql="""
            DROP TABLE IF EXISTS employees_temp;
            CREATE TABLE employees_temp (
                `Serial Number` INT PRIMARY KEY,
                `Company Name` VARCHAR(255),
                `Employee Markme` VARCHAR(255),
                `Description` TEXT,
                `Leave` INT
            );""",
    )

    @task
    def get_data():
        # NOTE: configure this as appropriate for your Airflow environment
        data_path = "/opt/airflow/dags/files/employees.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        url = "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/docs/tutorial/pipeline_example.csv"

        response = requests.request("GET", url)

        with open(data_path, "w") as file:
            file.write(response.text)

        mysql_hook = MySqlHook(mysql_conn_id="tutorial_mysql_conn", local_infile=True)
        conn = mysql_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            # Use MySQL LOAD DATA LOCAL INFILE to bulk-load the CSV into employees_temps
            cur.execute(f"""
                LOAD DATA LOCAL INFILE '{data_path}'
                INTO TABLE employees_temp
                FIELDS TERMINATED BY ',' ENCLOSED BY '"' 
                LINES TERMINATED BY '\\n'
                IGNORE 1 LINES
                (`Serial Number`, `Company Name`, `Employee Markme`, `Description`, `Leave`);
            """)
        conn.commit()

    @task
    def merge_data():
        query = """
            INSERT INTO employees (`Serial Number`, `Company Name`, `Employee Markme`, `Description`, `Leave`)
            SELECT DISTINCT `Serial Number`, `Company Name`, `Employee Markme`, `Description`, `Leave`
            FROM employees_temp AS src
            ON DUPLICATE KEY UPDATE
              `Employee Markme` = src.`Employee Markme`,
              `Description` = src.`Description`,
              `Leave` = src.`Leave`;
        """
        try:
            mysql_hook = MySqlHook(mysql_conn_id="tutorial_mysql_conn")
            conn = mysql_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            print(e)
            return 1

    [create_employees_table, create_employees_temp_table] >> get_data() >> merge_data()


dag = ProcessEmployees()
