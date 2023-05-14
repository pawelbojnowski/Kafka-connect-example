#!/bin/bash


#remove existing docker images
ids=$(docker ps -aqf name=kce_.)
for id in $(echo $ids | tr "\n" " "); do
  docker stop  $id
  docker container rm -f $id
done

#remove existing docker volumes
rm -rf ./volumes
rm -rf ./volumesData

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


#https://digitalis.io/blog/apache-cassandra/getting-started-with-kafka-cassandra-connector/
curl -s -H "Content-Type: application/json" -XPOST 'http://localhost:8083/connectors' -d '{
  "name": "cassandraSinkConnector",
  "config": {
    "connector.class": "io.confluent.connect.cassandra.CassandraSinkConnector",
    "tasks.max": "1",
    "topics": "user_topic",
    "cassandra.contact.points": "kce_cassandra",
    "connect.cassandra.port": "9042",
    "cassandra.keyspace": "kafka_connect_example2",
    "cassandra.write.mode": "Insert",
    "cassandra.username": "root",
    "cassandra.password": "root",
    "cassandra.export.route.query": "INSERT INTO user SELECT * FROM user_topic",

    "confluent.topic.replication.factor": 1,
    "cassandra.table.manage.enabled": "false",
    "cassandra.local.datacenter": "datacenter1",
    "confluent.topic.bootstrap.servers": "http://kce_kafka:9092"
  }
}
'  | json_pp


sleep 2

echo "\n----------------------------------------------------"
echo "List connectors:\n"

curl -s -H "Content-Type: application/json" -XGET 'http://localhost:8083/connectors'  | json_pp

echo "\n----------------------------------------------------"
echo "List topic:\n"
curl -s -H "Content-Type: application/vnd.kafka.v2+json" -XGET 'http://localhost:8082/topics' | json_pp








