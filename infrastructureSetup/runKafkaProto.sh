#!/bin/bash


#remove existing docker images
ids=$(docker ps -aqf name=kce_.)
for id in $(echo $ids | tr "\n" " "); do
  docker stop  $id
  docker container rm -f $id
done

#remove existing docker volumes
rm -rf ../volumes

#create docker
docker-compose -f docker-compose-proto.yml up --build -d --remove-orphans


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
      "table.whitelist" : "user",
      "incrementing.column.name":"id",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
      "value.converter.schema.registry.url":"http://kce_schema_registry:8081"
   }
}
'  | json_pp


curl -s -H "Content-Type: application/json" -XPOST 'http://localhost:8083/connectors' -d '{
   "name": "postgres.connector.source.user.query.timestamp",
   "config": {
     "topic.prefix": "postgres.connector.source.user.query.timestamp",
     "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
     "tasks.max": "1",
     "connection.url": "jdbc:postgresql://kce_postgres:5432/postgres",
     "connection.user": "postgres",
     "connection.password": "postgres",
     "mode": "timestamp",
     "query": "SELECT u.* FROM public.user u WHERE u.id > 1",
     "validate.non.null": "false"
   }
}
'  | json_pp

curl -s -H "Content-Type: application/json" -XPOST 'http://localhost:8083/connectors' -d '{
            "name": "postgres.connector.source.user.query.incrementing",
            "config": {
              "topic.prefix": "postgres.connector.source.user.query.incrementing",
              "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
              "tasks.max": "1",
              "connection.url": "jdbc:postgresql://kce_postgres:5432/postgres",
              "connection.user": "postgres",
              "connection.password": "postgres",
              "mode": "incrementing",
              "query": "Select t.* From (SELECT u.* FROM public.user u WHERE u.id > 1) t",
              "incrementing.column.name": "id",
              "validate.non.null": "false",
              "key.converter": "org.apache.kafka.connect.storage.StringConverter",
              "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
              "value.converter.schema.registry.url":"http://kce_schema_registry:8081"
            }
          }
'  | json_pp

curl -s -H "Content-Type: application/json" -XPOST 'http://localhost:8083/connectors' \
-d '{
      "name": "postgres.connector.sink.user.insert",
      "config": {
        "topics": "postgres.connector.sink.user.insert",
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
        "delete.enabled": "false",
        "schemas.enable": "false",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "fields.whitelist":"id,firstname,lastname,phone_number",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
        "value.converter.schema.registry.url":"http://kce_schema_registry:8081"
      }
    }'  | json_pp

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
        "delete.enabled": "false",
        "schemas.enable": "false",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "true",
        "fields.whitelist":"id,firstname,lastname,phone_number",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
        "value.converter.schema.registry.url":"http://kce_schema_registry:8081"
      }
    }'  | json_pp

curl -s -H "Content-Type: application/json" -XPOST 'http://localhost:8083/connectors' \
-d '{
      "name": "postgres.connector.sink.user.update",
      "config": {
        "topics": "postgres.connector.sink.user.update",
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
        "insert.mode": "upsert",
        "delete.enabled": "false",
        "schemas.enable": "false",
        "insert.mode": "upsert",
        "delete.enabled": "false",
        "pk.fields":"id",
        "pk.mode": "record_value",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "true",
        "fields.whitelist":"id,firstname,lastname,phone_number",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
        "value.converter.schema.registry.url":"http://kce_schema_registry:8081"
      }
    }'  | json_pp


curl -s -H "Content-Type: application/json" -XPOST 'http://localhost:8083/connectors' -d '
{
  "name": "cassandra_connector_sink_user",
  "config": {
      "connector.class": "com.datastax.oss.kafka.sink.CassandraSinkConnector",
      "tasks.max": "1",
      "topics": "cassandra_connector_sink_user",
      "contactPoints": "kce_cassandra",
      "auth.username": "root",
      "auth.password": "root",
      "loadBalancing.localDc": "datacenter1",
      "topic.cassandra_connector_sink_user.kafka_connect_example.client.mapping": "client_id=key, id=value.id, firstname=value.firstname, lastname=value.lastname, phone_number=value.phone_number",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
      "value.converter.schema.registry.url":"http://kce_schema_registry:8081",
      "key.converter.schemas.enable": false,
      "value.converter.schemas.enable": true,
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
      "value.converter.schema.registry.url":"http://kce_schema_registry:8081"
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








