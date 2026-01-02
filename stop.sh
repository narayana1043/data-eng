#!/bin/bash
set -e
PATH_PREFIX=${PATH_PREFIX:-"./"}

echo "Stopping Airflow services..."
docker compose -f ${PATH_PREFIX}services/airflow/docker-compose.yaml down
echo "Airflow services stopped."

echo "Stopping Kafka services..."
docker compose -f ${PATH_PREFIX}services/kafka/docker-compose.yaml down
echo "Kafka services stopped."

echo "Stopping Spark & Delta services..."
docker compose -f ${PATH_PREFIX}services/spark-delta/docker-compose.yaml down
echo "Spark & Delta services stopped."