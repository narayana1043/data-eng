#!/bin/bash
set -e

PATH_PREFIX=${PATH_PREFIX:-"./"}
# Set the versions
SCALA_VERSION=2.13
HADOOP_VERSION=3.3.6
SPARK_VERSION=4.0.1
KAFKA_VERSION=3.5.1   # Parameterized Kafka version
DELTA_VERSION=4.0.0   # Upgrade Delta version to 4.x

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
    echo "curl -fL -o $file $url"
    curl -fL -o "$file" "$url"
  fi
}

####################################
# Airflow
####################################
start_airflow() {
  COMPOSE_FILE=${PATH_PREFIX}services/airflow/docker-compose-standalone.yaml

  echo "Starting Airflow services..."
  echo "copying hadoop config files to airflow service..."
  docker compose -f ${COMPOSE_FILE} down
  sleep 3
  mkdir -p ${PATH_PREFIX}services/airflow/tmp/hadoop-conf/
  cp -r ${PATH_PREFIX}services/spark/*.xml ${PATH_PREFIX}services/airflow/tmp/hadoop-conf/
  if ! [ -f services/airflow/tmp/pyspark-${SPARK_VERSION}.tar.gz ]; then
    if [ -f services/spark/tmp/pyspark-${SPARK_VERSION}.tar.gz ]; then
      cp services/spark/tmp/pyspark-${SPARK_VERSION}.tar.gz services/airflow/tmp/pyspark-${SPARK_VERSION}.tar.gz
    else
      pip download --no-deps --dest services/airflow/tmp pyspark==${SPARK_VERSION} delta-spark==3.1.0
      mkdir -p services/spark/tmp/
      cp services/airflow/tmp/pyspark-${SPARK_VERSION}.tar.gz services/spark/tmp/pyspark-${SPARK_VERSION}.tar.gz
    fi
  fi

  docker compose -f ${COMPOSE_FILE} up --build -d
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
  
  # Download Spark
  download_if_missing services/spark/tmp/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
      https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz

  # Download Hadoop if missing
  download_if_missing services/spark/tmp/hadoop-${HADOOP_VERSION}.tar.gz \
      https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz

  # Download Kafka SQL connector for Spark 4.0.1 if missing (ensure it's compatible with Spark 4.x)
  download_if_missing services/spark/tmp/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar

  # Download Kafka token provider for Spark 4.0.1 if missing
  download_if_missing services/spark/tmp/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar

  # Download Kafka clients with parameterized Kafka version
  download_if_missing services/spark/tmp/kafka-clients-${KAFKA_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar

  # Download Delta Spark with parameterized Delta version if missing
  download_if_missing services/spark/tmp/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar
  
  # Download commons-pool2
  download_if_missing services/spark/tmp/commons-pool2-${SCALA_VERSION}.1-bin.tar.gz \
      https://dlcdn.apache.org//commons/pool/binaries/commons-pool2-${SCALA_VERSION}.1-bin.tar.gz
  tar -xvf services/spark/tmp/commons-pool2-${SCALA_VERSION}.1-bin.tar.gz \
      --strip-components=1 \
      -C services/spark/tmp/ commons-pool2-${SCALA_VERSION}.1/commons-pool2-${SCALA_VERSION}.1.jar


  if ! [ -f services/spark/tmp/pyspark-${SPARK_VERSION}.tar.gz ]; then
    if [ -f services/airflow/tmp/pyspark-${SPARK_VERSION}.tar.gz ]; then
      cp services/airflow/tmp/pyspark-${SPARK_VERSION}.tar.gz services/spark/tmp/pyspark-${SPARK_VERSION}.tar.gz
    else
      pip download --no-deps --dest services/spark/tmp pyspark==${SPARK_VERSION} delta-spark==4.0
      mkdir -p services/airflow/tmp/
      cp services/spark/tmp/pyspark-${SPARK_VERSION}.tar.gz services/airflow/tmp/pyspark-${SPARK_VERSION}.tar.gz
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
