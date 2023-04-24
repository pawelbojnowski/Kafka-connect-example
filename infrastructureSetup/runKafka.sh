#!/bin/bash


#remove existing docker images
ids=$(docker ps -aqf name=kce_.)
for id in $(echo $ids | tr "\n" " "); do
  docker stop  $id
  docker container rm -f $id
done

#remove existing docker volumes
rm -rf ./volumes

#create docker
docker-compose up --build -d --remove-orphans


while [[ $(curl -s -H "Content-Type: application/json" -XGET 'http://localhost:8083/connectors') != "[]" ]];
do
  printf "."
  sleep 1
done
sleep 3
echo "\n----------------------------------------------------"
echo "Create connectors:\n"

curl -s -H "Content-Type: application/json" -XPOST 'http://localhost:8083/connectors' -d '{
   "name":"postgres.connector.source",
   "config":{
      "topic.prefix":"postgres.connector.source.",
      "connector.class":"io.confluent.connect.jdbc.JdbcSourceConnector",
      "tasks.max":"1",
      "connection.url":"jdbc:postgresql://kce_postgres:5432/postgres",
      "connection.user":"postgres",
      "connection.password":"postgres",
      "mode":"incrementing",
      "incrementing.column.name":"id"
   }
}
'  | json_pp

curl -s -H "Content-Type: application/json" -XPOST 'http://localhost:8083/connectors' \
-d '{
      "name": "postgres.connector.sink.client",
      "config": {
        "topics": "postgres.connector.sink.client",

        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "tasks.max": "1",
        "connection.url": "jdbc:postgresql://kce_postgres:5432/postgres",
        "connection.user": "postgres",
        "connection.password": "postgres",
        "connection.ds.pool.size": 5,
        "insert.mode.databaselevel": true,
        "table.name.format": "client",
        "auto.create": "false",
        "auto.evolve": "true",
        "insert.mode": "insert",
        "delete.enabled": "true",
        "schemas.enable": "false",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "true",
        "fields.whitelist":"id,firstname,lastname,phone_number"
      }
    }'  | json_pp




sleep 2

echo "\n----------------------------------------------------"
echo "List connectors:\n"

curl -s -H "Content-Type: application/json" -XGET 'http://localhost:8083/connectors'  | json_pp

echo "\n----------------------------------------------------"
echo "List topic:\n"
curl -s -H "Content-Type: application/json" -XGET 'http://localhost:8082/topics' | json_pp








