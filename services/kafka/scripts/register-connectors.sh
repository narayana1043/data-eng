#!/bin/bash
set -e

echo "Waiting for Kafka Connect to be fully ready..."

until curl -s http://localhost:8083/connector-plugins | grep -q "PostgresConnector"; do
  echo "Kafka Connect not ready yet..."
  sleep 5
done

echo "Registering PostgreSQL Connector..."
curl -i -X POST \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  http://localhost:8083/connectors \
  -d @/scripts/connectors/postgres-source.json

echo "Registering MySQL Connector..."
curl -i -X POST \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  http://localhost:8083/connectors \
  -d @/scripts/connectors/mysql-source.json

echo "Connectors registered successfully."
