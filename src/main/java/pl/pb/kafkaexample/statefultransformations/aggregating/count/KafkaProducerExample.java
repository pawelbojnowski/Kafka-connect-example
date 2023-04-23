package pl.pb.kafkaexample.statefultransformations.aggregating.count;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statefultransformations.aggregating.count.KafkaConfig.getProducerConfig;

public class KafkaProducerExample {

	public static void main(final String[] args) {

		// create the producer
		final KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerConfig());

		List.of("REGULAR_USER", "ADMIN_USER", "REGULAR_USER", "ADMIN_USER", "AGENT_USER", "UNKNOWN_USER", "REGULAR_USER", "ADMIN_USER", "UNKNOWN_USER").forEach(userType -> {

			// create a producer record
			final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(KafkaConfig.INPUT_TOPIC_1, userType, "Data for user with type: " + userType);

			// send data - asynchronous
			producer.send(producerRecord);

			println("Sent message: " + producerRecord);
		});

		// flush data - synchronous
		producer.flush();

		// flush and close producer
		producer.close();
	}
}
