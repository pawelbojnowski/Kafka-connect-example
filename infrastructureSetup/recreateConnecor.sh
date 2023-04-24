#!/bin/bash

curl -s -H "Content-Type: application/json" -XPOST 'http://localhost:8083/connectors' \
-d '{
      "name": "postgres.connector.sink.user11",
      "config": {
        "topics": "postgres.connector.sink.user11",

        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "tasks.max": "1",
        "connection.url": "jdbc:postgresql://kce_postgres:5432/postgres",
        "connection.user": "postgres",
        "connection.password": "postgres",
        "connection.ds.pool.size": 5,
        "insert.mode.databaselevel": true,
        "table.name.format": "user",
        "auto.create": "false",
        "auto.evolve": "true",
        "insert.mode": "insert",
        "delete.enabled": "true",
        "schemas.enable": "false",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "true",
        "fields.whitelist":"id,firstname,lastname,phone_number",
        "kafkastore.init.timeout.ms":1000
      }
    }'  | json_pp




sleep 5

echo "\n----------------------------------------------------"
echo "List connectors:\n"

curl -s -H "Content-Type: application/json" -XGET 'http://localhost:8083/connectors'  | json_pp

echo "\n----------------------------------------------------"
echo "List topic:\n"
curl -s -H "Content-Type: application/json" -XGET 'http://localhost:8082/topics' | json_pp








