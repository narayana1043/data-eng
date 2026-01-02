#!/bin/bash
set -e

PATH_PREFIX=${PATH_PREFIX:-"./"}
IS_WSL=false
if grep -qi microsoft /proc/version; then
  IS_WSL=true
  echo "WSL environment detected"
fi

####################################
# Common permission setup
####################################
fix_permissions() {
  if [ "$IS_WSL" = true ]; then
    sudo chown -R "$(id -u)":"$(id -g)" ./data ./services ./scripts || true
    sudo chmod -R 775 ./data ./services ./scripts || true
  else
    sudo chown -R "$(id -un)":"$(id -gn)" .
    sudo chmod -R ugo+rw .
  fi
}

####################################
# Airflow
####################################
start_airflow() {
  echo "Starting Airflow services..."
  docker compose -f ${PATH_PREFIX}services/airflow/docker-compose.yaml down
  sleep 3
  docker compose -f ${PATH_PREFIX}services/airflow/docker-compose.yaml up --build -d
  echo "Airflow services started."

  read -r -p "Trigger postgres_sample_db_restore DAG? (y/n): " trigger_dag
  if [[ "$trigger_dag" == "y" ]]; then
    sudo mkdir -p ./data/airflow/{logs,dags,plugins}
    sudo chown -R $(id -u):0 ./data/airflow/{logs,dags,plugins}
    sudo chmod -R 775 ./data/airflow/{logs,dags,plugins}

    docker compose -f ${PATH_PREFIX}services/airflow/docker-compose.yaml exec airflow-apiserver \
      airflow dags trigger postgres_sample_db_restore

    echo "Postgres sample DB DAG triggered."
  fi
}

####################################
# Kafka
####################################
start_kafka() {
  echo "Starting Kafka services..."
  docker compose -f ${PATH_PREFIX}services/kafka/docker-compose.yaml down
  sleep 3
  docker compose -f ${PATH_PREFIX}services/kafka/docker-compose.yaml up --build -d
  echo "Kafka services started."

  read -r -p "Register Debezium PostgreSQL connector? (y/n): " register_connector
  if [[ "$register_connector" == "y" ]]; then
    docker compose -f ${PATH_PREFIX}services/kafka/docker-compose.yaml exec debezium \
      curl -i -X POST \
      -H "Accept:application/json" \
      -H "Content-Type:application/json" \
      http://localhost:8083/connectors \
      -d @/scripts/connectors/postgres-dvdrental-source.json

    echo "Debezium connector registered."
  fi
}

####################################
# Spark
####################################
start_spark() {
  echo "Starting Spark & Delta services..."
  docker compose -f ${PATH_PREFIX}services/spark/docker-compose.yaml down
  sleep 3
  docker compose -f ${PATH_PREFIX}services/spark/docker-compose.yaml up -d
  echo "Spark & Delta services started."
}

####################################
# Menu
####################################
fix_permissions

echo "=============================="
echo " Select services to start "
echo "=============================="
echo "1) Airflow"
echo "2) Kafka"
echo "3) Spark & Delta"
echo "4) All Services"
echo "5) Exit"
echo "=============================="

read -r -p "Enter choice [1-5]: " choice

case $choice in
  1)
    start_airflow
    ;;
  2)
    start_kafka
    ;;
  3)
    start_spark
    ;;
  4)
    start_airflow
    start_kafka
    start_spark
    ;;
  5)
    echo "Exiting..."
    exit 0
    ;;
  *)
    echo "Invalid choice!"
    exit 1
    ;;
esac

fix_permissions
echo "✅ Selected services started successfully."
