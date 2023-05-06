package pl.pb.kafkaconnectexample.postgress.avro;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import pl.pb.kafkamodel.avro.User;

import java.time.Duration;
import java.util.Arrays;

import static pl.pb.kafkaconnectexample.postgress.avro.KafkaConfig.POSTGRES_SOURCED_USER;
import static pl.pb.kafkaconnectexample.postgress.config.Commons.println;

public class KafkaConsumerExample {

	public static void main(final String[] args) {

		// create consumer
		final KafkaConsumer<String, User> consumer = KafkaConfig.<String, User>getConsumer();

		// add subscribed topic(s)
		consumer.subscribe(Arrays.asList(POSTGRES_SOURCED_USER));

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
