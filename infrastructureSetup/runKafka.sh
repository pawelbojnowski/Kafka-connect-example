#!/bin/bash


#remove existing docker images
ids=$(docker ps -aqf name=kce_.)
for id in $(echo $ids | tr "\n" " "); do
  docker stop  $id
  docker container rm -f $id
done

#remove existing docker volumes
rm -rf ./db

#create docker
docker-compose up --build -d --remove-orphans


while [[ $(curl -s -H "Content-Type: application/json" -XGET 'http://localhost:8083/connectors') != "[]" ]];
do
  printf "."
  sleep 1
done

echo "\n----------------------------------------------------"
echo "Create connectors:\n"

curl -s -H "Content-Type: application/json" -XPOST 'http://localhost:8083/connectors' -d '{
   "name":"postgres.connector",
   "config":{
      "connector.class":"io.confluent.connect.jdbc.JdbcSourceConnector",
      "tasks.max":"1",
      "connection.url":"jdbc:postgresql://kce_postgres:5432/postgres",
      "connection.user":"postgres",
      "connection.password":"postgres",
      "mode":"incrementing",
      "incrementing.column.name":"id",
      "topic.prefix":"postgres.sourced."
   }
}
'  | json_pp

sleep 2

echo "\n----------------------------------------------------"
echo "List connectors:\n"

curl -s -H "Content-Type: application/json" -XGET 'http://localhost:8083/connectors'  | json_pp

echo "\n----------------------------------------------------"
echo "List topic:\n"
curl -s -H "Content-Type: application/json" -XGET 'http://localhost:8082/topics' | json_pp








