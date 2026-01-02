#!/bin/bash
set -e
PATH_PREFIX=${PATH_PREFIX:-"./"}

sudo chown -R "$(id -un)":"$(id -gn)" .
sudo chmod -R ugo+rw .


####################################
# This script starts Airflow services and ensures
# that the necessary connections are configured.
####################################
echo "Restarting Airflow services..."
docker compose -f ${PATH_PREFIX}services/airflow/docker-compose.yaml down
sleep 3 
docker compose -f ${PATH_PREFIX}services/airflow/docker-compose.yaml up --build -d
echo "Airflow services restarted."

echo "Check to make sure connections/roles are configured on airflow else manually create connections/roles in Airflow UI."
read -r -p "Press Enter after confirming connections/roles in Airflow UI ..." </dev/tty

####################################
# Start Kafka services
####################################
echo "Starting Kafka services..."
docker compose -f ${PATH_PREFIX}services/kafka/docker-compose.yaml down
sleep 3
docker compose -f ${PATH_PREFIX}services/kafka/docker-compose.yaml up --build -d
echo "Kafka services started."

####################################
# Run Postgres sample db DAG in airflow to create sample data
####################################
sudo mkdir -p ./data/airflow/{logs,dags,plugins}
sudo chown -R $(id -u):0 ./data/airflow/{logs,dags,plugins}
sudo chmod -R 775 ./data/airflow/{logs,dags,plugins}

echo "Triggering Postgres sample db DAG in Airflow..."
docker compose -f ./services/airflow/docker-compose.yaml exec airflow-apiserver \
  airflow dags trigger postgres_sample_db_restore
echo "Postgres sample db DAG triggered."

####################################
# Register Kafka connections in Airflow
####################################
echo "Registering PostgreSQL dvdrental Connector..."
docker compose -f services/kafka/docker-compose.yaml exec debezium \
  /bin/bash curl -i -X POST \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  http://localhost:8083/connectors \
  -d @/scripts/connectors/postgres-dvdrental-source.json
  echo "PostgreSQL dvdrental Connector registered successfully."

####################################
# Start Spark & Delta services
####################################
echo "Starting Spark & Delta services..."
docker compose -f ${PATH_PREFIX}services/spark-delta/docker-compose.yaml up -d
echo "Spark & Delta services started."

####################################
# Restore permissions
####################################
echo "All services are up and running. Please check the Airflow UI and Kafka setup."
# Run any necessary permission adjustments if sudo chown -R "$(id -un)":"$(id -gn)" . fails
# printf '%s ALL=(ALL) ALL\n' "$(id -un)" | sudo tee "/etc/sudoers.d/$(id -un)" > /dev/null \
#     && sudo chmod 0440 "/etc/sudoers.d/$(id -un)" \
#     && sudo visudo -cf "/etc/sudoers.d/$(id -un)" 
sudo chown -R "$(id -un)":"$(id -gn)" .
sudo chmod -R ugo+rw .