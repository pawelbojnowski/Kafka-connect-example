package pl.pb.kafkaconnectexample.proto;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import pl.pb.kafkamodel.user.User;

import java.time.Duration;
import java.util.List;

import static pl.pb.kafkaconnectexample.config.Commons.println;
import static pl.pb.kafkaconnectexample.proto.KafkaConfig.POSTGRES_SOURCED_USER;
import static pl.pb.kafkaconnectexample.proto.KafkaConfig.getConsumer;

public class KafkaConsumerExample {

	public static void main(final String[] args) {

		// create consumer
		final KafkaConsumer<String, User> consumer = getConsumer();

		// add subscribed topic(s)
		consumer.subscribe(List.of(POSTGRES_SOURCED_USER));

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
