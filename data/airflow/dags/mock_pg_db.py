from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import requests
import psycopg2
from datetime import datetime
import zipfile
import subprocess


default_args = {
    'owner': 'airflow',
}

def download_sample_db():
    """Download PostgreSQL sample database SQL file"""
    url = "https://neon.com/postgresqltutorial/dvdrental.zip"
    response = requests.get(url)
    with open('/tmp/dvdrental.zip', 'wb') as f:
        f.write(response.content)
    print("Sample database downloaded")

def extract():
    """Extract and load data into PostgreSQL"""
    
    # Extract zip file
    with zipfile.ZipFile('/tmp/dvdrental.zip', 'r') as zip_ref:
        zip_ref.extractall('/tmp/postgres_sample_db/')
        
    print("Sample database loaded")

with DAG(
    'postgres_sample_db_load',
    default_args=default_args,
    catchup=False,
) as dag:

    download = PythonOperator(
        task_id='download_sample_db',
        python_callable=download_sample_db,
    )

    extract = PythonOperator(
        task_id='load_sample_db',
        python_callable=extract,
    )

    restore_db = SQLExecuteQueryOperator(
        task_id='run_sql',
        conn_id="tutorial_pg_conn",
        sql='/tmp/postgres_sample_db/restore.sql',
    )

    download >> extract >> restore_db