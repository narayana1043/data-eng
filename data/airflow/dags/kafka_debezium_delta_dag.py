from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="kafka_debezium_to_delta_dag",
    description="Submit Debezium CDC job to existing Spark cluster",
    start_date=datetime(2025, 1, 1),
    schedule=None,   # Streaming job
    catchup=False,
    max_active_runs=1,
    tags=["spark", "kafka", "debezium", "delta"]
) as dag:

    spark_cdc_job = SparkSubmitOperator(
        task_id="submit_spark_cdc_job",

        # Path INSIDE Airflow container
        application="/opt/spark/jobs/spark_kafka_debezium_to_delta.py",
        
        # FORCE Standalone mode (overrides everything)
        conn_id = 'spark_standalone',

        # IMPORTANT: submit to existing Spark cluster
        conf={
            # Delta configs
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        },

        # Packages NOT needed if already baked into Spark image
        # packages="io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",

        application_args=[
            "--kafka-bootstrap", "kafka:29092",
            "--topic", "postgres_dvdrental.public.film",
            "--delta-table", "dvdrental_delta.film",
            "--checkpoint", "/delta/_checkpoints/film"
        ],

        name="kafka-debezium-delta-cdc",
        executor_cores=2,
        executor_memory="2g",
        driver_memory="1g",
        verbose=True
    )

    spark_cdc_job
