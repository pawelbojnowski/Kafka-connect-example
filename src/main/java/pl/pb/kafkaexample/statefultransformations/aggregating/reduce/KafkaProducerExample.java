package pl.pb.kafkaexample.statefultransformations.aggregating.reduce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

import static pl.pb.kafkaexample.config.Commons.println;

public class KafkaProducerExample {

	public static void main(final String[] args) {

		// create the producer
		final KafkaProducer<String, Long> producer = new KafkaProducer<>(KafkaConfig.getProducerConfig());

		List.of("REGULAR_USER", "ADMIN_USER", "REGULAR_USER", "ADMIN_USER", "AGENT_USER", "UNKNOWN_USER", "REGULAR_USER", "ADMIN_USER", "UNKNOWN_USER").forEach(userType -> {

			// create a producer record
			final ProducerRecord<String, Long> producerRecord = new ProducerRecord<>(KafkaConfig.INPUT_TOPIC_1, userType, 1L);

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
