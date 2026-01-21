from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from dvd_rental_db_datasets import dvdrental_db_ready_ds

with DAG(
    dag_id="dvd_rental_db_vectorize",
    start_date=datetime(2025, 1, 1),
    schedule=[dvdrental_db_ready_ds],  # dataset-driven
    catchup=False,
) as dag:
    
    create_embedding_column = SQLExecuteQueryOperator(
        task_id="create_embedding_column",
        conn_id="postgres_dvdrental_db",
        sql="CREATE EXTENSION IF NOT EXISTS vector; ALTER TABLE film ADD COLUMN IF NOT EXISTS embedding vector(384);",
        hook_params={"schema": "public"},
        split_statements=True,
    )

    generate_embeddings = DockerOperator(
        task_id="generate_embeddings",
        image="local-embedding:latest",
        command="python /scripts/dvd_rental_embed_films.py",
        docker_url="tcp://host.docker.internal:2375",
        network_mode="data-engine-network",
        auto_remove='success',
        mount_tmp_dir=False,
        mounts=[Mount(source=r"C:\\Users\\veera\\code\\data-eng\\services\\local-embedding\\scripts", target="/scripts", type="bind", read_only=False)],
    )

    index_embedding_column = SQLExecuteQueryOperator(
        task_id="index_embedding_column",
        conn_id="postgres_dvdrental_db",
        sql="""CREATE INDEX film_embedding_idx
               ON film
               USING ivfflat (embedding vector_cosine_ops)
               WITH (lists = 100);""",
        hook_params={"schema": "public"},
    )

    create_embedding_column >> generate_embeddings >> index_embedding_column
