package pl.pb.kafkaexample.statefultransformations.aggregating.reduce_windowed;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Random;

import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.config.Commons.sleep;

public class KafkaProducerExample {

	public static void main(final String[] args) {

		// create the producer
		final KafkaProducer<String, Long> producer = new KafkaProducer<>(KafkaConfig.getProducerConfig());

		final List<String> userTypes = List.of("REGULAR_USER");

		final Random random = new Random();

		for (int i = 0; i < 150; i++) {
			// get user type
			final String userType = userTypes.get(random.nextInt(userTypes.size()));

			// create a producer record
			final ProducerRecord<String, Long> producerRecord = new ProducerRecord<>(KafkaConfig.INPUT_TOPIC_1, userType, 1l);

			// send data - asynchronous
			producer.send(producerRecord);

			println("Sent message: " + producerRecord);
			sleep(50 + random.nextInt(100));
		}

		// flush data - synchronous
		producer.flush();

		// flush and close producer
		producer.close();
	}
}
