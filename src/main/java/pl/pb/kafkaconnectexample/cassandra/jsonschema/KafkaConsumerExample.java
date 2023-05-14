package pl.pb.kafkaconnectexample.cassandra.jsonschema;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;

import static pl.pb.kafkaconnectexample.cassandra.config.Commons.println;
import static pl.pb.kafkaconnectexample.cassandra.jsonschema.KafkaConfig.CASSANDRA_SOURCED_USER;
import static pl.pb.kafkaconnectexample.cassandra.jsonschema.KafkaConfig.getConsumer;

public class KafkaConsumerExample {

	public static void main(final String[] args) {

		// create consumer
		final KafkaConsumer<String, Long> consumer = getConsumer();

		// add subscribed topic(s)
		consumer.subscribe(Arrays.asList(CASSANDRA_SOURCED_USER));

		// consume data
		while (true) {
			consumer.poll(Duration.ofMillis(100))
					.forEach(consumerRecord -> println("Topic: %s,\n Key: %s,\n Value: %-10s,\n Partition: %s,\n Offset: %s\n",
							consumerRecord.topic(),
							consumerRecord.key(),
							consumerRecord.value(),
							consumerRecord.partition(),
							consumerRecord.offset()
					));
		}
	}

}
