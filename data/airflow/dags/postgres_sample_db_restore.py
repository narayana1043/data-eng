from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import requests
from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator

default_args = {'owner': 'airflow'}

@dag(
    dag_id='postgres_sample_db_restore',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    template_searchpath=['/tmp/postgres_sample_db']
)
def postgres_sample_db_load():

    @task
    def download_dvdrental_db():
        url = "https://neon.com/postgresqltutorial/dvdrental.zip"
        response = requests.get(url)
        with open('/tmp/dvdrental.zip', 'wb') as f:
            f.write(response.content)
        print("Sample database downloaded")

    download = download_dvdrental_db()

    extract = BashOperator(
            task_id='extract_dvdrental_db',
            bash_command='mkdir -p /tmp/postgres_sample_db && tar -xvzf /tmp/dvdrental.zip -C /tmp/postgres_sample_db && echo "Sample database extracted"',
        )

    prepare_restore_scripts = BashOperator(
        task_id='prepare_restore_scripts',
        bash_command=(
            "sed -i 's|DROP DATABASE|DROP DATABASE IF EXISTS|g' /tmp/postgres_sample_db/restore.sql "
            "&& sed -i \"s|LC_COLLATE = 'English_United States.1252' LC_CTYPE = 'English_United States.1252'|LC_COLLATE = 'en_US.utf8' LC_CTYPE = 'en_US.utf8'|g\" /tmp/postgres_sample_db/restore.sql "
            "&& sed -i 's/\\<postgres\\>/airflow/g' /tmp/postgres_sample_db/restore.sql "
            "&& head -n 34 /tmp/postgres_sample_db/restore.sql > /tmp/postgres_sample_db/create_db.sql "
            "&& echo 'ALTER SYSTEM SET wal_level = logical;' >> /tmp/postgres_sample_db/create_db.sql "
            "&& tail -n +36 /tmp/postgres_sample_db/restore.sql > /tmp/postgres_sample_db/create_ddl.sql "
            "&& sed -i 's|\$\$PATH\$\$|/tmp/postgres_sample_db|g' /tmp/postgres_sample_db/create_ddl.sql "
            "&& sed -i 's|^COPY|\\\\copy|g' /tmp/postgres_sample_db/create_ddl.sql"
        ),
    )

    restore_dvdrental_db = BashOperator(
        task_id='restore_dvdrental_db',
        bash_command=(
            'PGPASSWORD="${PGPASSWORD:-airflow}" psql -h "${PGHOST:-postgres}" -U "${PGUSER:-airflow}" -f /tmp/postgres_sample_db/create_db.sql'
            ' && PGPASSWORD="${PGPASSWORD:-airflow}" psql -h "${PGHOST:-postgres}" -U "${PGUSER:-airflow}" -d dvdrental -f /tmp/postgres_sample_db/create_ddl.sql'
        ),
    )

    download >> extract >> prepare_restore_scripts >> restore_dvdrental_db

dag = postgres_sample_db_load()
