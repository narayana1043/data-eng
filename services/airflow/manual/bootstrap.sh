##################################
# API calls only will work in NO AUTH mode.
# NoAuth mode is not working in local setup with current Airflow version (3.1.5).
# This script is for demonstration purposes only.
# Manually create the connections via Airflow UI in local setup.
##################################
# Bootstrap script for Airflow in NO AUTH mode

#!/bin/bash
set -e

API_URL="http://airflow-webserver:8080/api/v2"

echo "🚀 Bootstrapping Airflow (NO AUTH mode)"

# ---------------------------------------------------
# CREATE POSTGRES CONNECTION
# ---------------------------------------------------
echo "➡️ Creating Postgres connection"

curl -sf \
  -H "Content-Type: application/json" \
  -X POST ${API_URL}/connections \
  -d '{
    "connection_id": "postgres_airflow_db",
    "conn_type": "postgres",
    "host": "postgres",
    "login": "airflow",
    "password": "airflow",
    "schema": "airflow",
    "port": 5432
  }' || echo "ℹ️ Postgres connection already exists"

# ---------------------------------------------------
# CREATE MYSQL CONNECTION
# ---------------------------------------------------
echo "➡️ Creating MySQL connection"

curl -sf \
  -H "Content-Type: application/json" \
  -X POST ${API_URL}/connections \
  -d '{
    "connection_id": "mysql_airflow_db",
    "conn_type": "mysql",
    "host": "mysql",
    "login": "airflow",
    "password": "airflow",
    "schema": "airflow",
    "port": 3306
  }' || echo "ℹ️ MySQL connection already exists"

echo "🎉 Airflow bootstrap completed successfully (NO AUTH)"
