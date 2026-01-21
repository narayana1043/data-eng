from airflow.datasets import Dataset

dvdrental_db_ready_ds = Dataset('checkpoint://dvdrental_db/ready')