package pl.pb.kafkaconnectexample.proto;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import pl.pb.kafkamodel.user.User;

import java.time.Duration;
import java.util.Arrays;

import static pl.pb.kafkaconnectexample.config.Commons.println;
import static pl.pb.kafkaconnectexample.proto.KafkaConfig.POSTGRES_SOURCED_USER;
import static pl.pb.kafkaconnectexample.proto.KafkaConfig.getConsumer;

public class KafkaConsumerExample {

	public static void main(final String[] args) {

		// create consumer
		final KafkaConsumer<String, User> consumer = getConsumer();

		// add subscribed topic(s)
		consumer.subscribe(Arrays.asList(POSTGRES_SOURCED_USER));

		// consume data
		while (true) {
			consumer.poll(Duration.ofMillis(100))
					.forEach(consumerRecord -> println("Topic: %s, Key: %s, Value: %-10s, Partition: %s, Offset: %s",
							consumerRecord.topic(),
							consumerRecord.key(),
							consumerRecord.value().getId(),
							consumerRecord.partition(),
							consumerRecord.offset()
					));
		}
	}

}
