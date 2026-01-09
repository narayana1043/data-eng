#!/bin/bash
set -e

PATH_PREFIX=${PATH_PREFIX:-"./"}

docker network inspect data-engine-network >/dev/null 2>&1 \
|| docker network create data-engine-network

####################################
# Common permission setup
####################################
fix_permissions() {
  sudo chown -R "$(id -un)":"$(id -gn)" .
  sudo chmod -R ugo+rw .
}

#####################################
### Utility to download file if missing
####################################

download_if_missing () {
  local file=$1
  local url=$2

  if [[ -f "$file" ]]; then
    echo "Skipping download: $file already exists."
  else
    curl -fL -o "$file" "$url"
  fi
}

####################################
# Airflow
####################################
start_airflow() {
  echo "Starting Airflow services..."
  echo "copying hadoop config files to airflow service..."
  docker compose -f ${PATH_PREFIX}services/airflow/docker-compose.yaml down
  sleep 3
  mkdir -p ${PATH_PREFIX}services/airflow/tmp/hadoop-conf/
  cp -r ${PATH_PREFIX}services/spark/*.xml ${PATH_PREFIX}services/airflow/tmp/hadoop-conf/
  if ! [ -f services/airflow/tmp/pyspark-3.5.1.tar.gz ]; then
    if [ -f services/spark/tmp/pyspark-3.5.1.tar.gz ]; then
      cp services/spark/tmp/pyspark-3.5.1.tar.gz services/airflow/tmp/pyspark-3.5.1.tar.gz
    else
      pip download --no-deps --dest services/airflow/tmp pyspark==3.5.1 delta-spark==3.1.0
      cp services/airflow/tmp/pyspark-3.5.1.tar.gz services/spark/tmp/pyspark-3.5.1.tar.gz
    fi
  fi

  docker compose -f ${PATH_PREFIX}services/airflow/docker-compose.yaml up --build -d
  echo "Airflow services started."
  rm -rf ${PATH_PREFIX}services/airflow/tmp/hadoop-conf/

  echo "removing copied hadoop config files from airflow service..."

  read -r -p "Trigger postgres_sample_db_restore DAG? (y/n): " trigger_dag
  if [[ "$trigger_dag" == "y" ]]; then
    sudo mkdir -p ./data/airflow/{logs,dags,plugins}
    sudo chown -R $(id -u):0 ./data/airflow/{logs,dags,plugins}
    sudo chmod -R 775 ./data/airflow/{logs,dags,plugins}

    docker compose -f ${PATH_PREFIX}services/airflow/docker-compose.yaml exec airflow-webserver \
      airflow dags trigger postgres_sample_db_restore

    echo "Postgres sample DB DAG triggered."
  fi

  docker ps -aq -f status=exited -f label=com.docker.compose.project=airflow | xargs -r docker rm

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
    sleep 80
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
  download_if_missing services/spark/tmp/hadoop-3.3.6.tar.gz \
    https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz

  download_if_missing services/spark/tmp/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar

  download_if_missing services/spark/tmp/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar

  download_if_missing services/spark/tmp/kafka-clients-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar

  download_if_missing services/spark/tmp/delta-spark_2.12-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar

  download_if_missing services/spark/tmp/delta-storage-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar
  
  download_if_missing services/spark/tmp/commons-pool2-2.12.0.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

  if ! [ -f services/spark/tmp/pyspark-3.5.1.tar.gz ]; then
    if [ -f services/airflow/tmp/pyspark-3.5.1.tar.gz ]; then
      cp services/airflow/tmp/pyspark-3.5.1.tar.gz services/spark/tmp/pyspark-3.5.1.tar.gz
    else
      pip download --no-deps --dest services/spark/tmp pyspark==3.5.1 delta-spark==3.1.0
      cp services/spark/tmp/pyspark-3.5.1.tar.gz services/airflow/tmp/pyspark-3.5.1.tar.gz
    fi
  fi

  docker compose -f ${PATH_PREFIX}services/spark/docker-compose.yaml down
  sleep 3
  docker compose -f ${PATH_PREFIX}services/spark/docker-compose.yaml up -d spark
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
