package pl.pb.kafkaconnectexample.cassandra.proto;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import pl.pb.kafkamodel.proto.User;

import java.time.Duration;
import java.util.Arrays;

import static pl.pb.kafkaconnectexample.cassandra.config.Commons.println;
import static pl.pb.kafkaconnectexample.cassandra.proto.KafkaConfig.CASSANDRA_SOURCED_USER;

public class KafkaConsumerExample {

	public static void main(final String[] args) {

		// create consumer
		final KafkaConsumer<String, User> consumer = KafkaConfig.<String, User>getConsumer();

		// add subscribed topic(s)
		consumer.subscribe(Arrays.asList(CASSANDRA_SOURCED_USER));

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
