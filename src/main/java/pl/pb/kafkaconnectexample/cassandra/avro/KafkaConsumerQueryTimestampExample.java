package pl.pb.kafkaconnectexample.cassandra.avro;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import pl.pb.kafkamodel.avro.User;

import java.time.Duration;
import java.util.Arrays;

import static pl.pb.kafkaconnectexample.cassandra.avro.KafkaConfig.CASSANDRA_CONNECTOR_SOURCE_USER_QUERY_TIMESTAMP;
import static pl.pb.kafkaconnectexample.cassandra.config.Commons.println;

public class KafkaConsumerQueryTimestampExample {

	public static void main(final String[] args) {

		// create consumer
		final KafkaConsumer<String, User> consumer = KafkaConfig.<String, User>getConsumer();

		// add subscribed topic(s)
		consumer.subscribe(Arrays.asList(CASSANDRA_CONNECTOR_SOURCE_USER_QUERY_TIMESTAMP));

		// consume data
		while (true) {
			consumer.poll(Duration.ofMillis(100))
					.forEach(consumerRecord -> println("Topic: %s,\nKey: %s,\nValue: %s,\nPartition: %s,\nOffset: %s\n",
							consumerRecord.topic(),
							consumerRecord.key(),
							consumerRecord.value().toString().replace("\n", ", "),
							consumerRecord.partition(),
							consumerRecord.offset()
					));
		}
	}

}
