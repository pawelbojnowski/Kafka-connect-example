package pl.pb.kafkaexample.statelesstransformations.cogroup;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

import static pl.pb.kafkaexample.config.Commons.sleep;
import static pl.pb.kafkaexample.statelesstransformations.cogroup.KafkaConfig.getProducerConfig;

public class KafkaProducerExample {

	public static void main(final String[] args) {

		// create the producer
		final KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerConfig());

		final Random random = new Random();
		for (int i = 0; i < 100; i++) {

			extracted(random, KafkaConfig.INPUT_TOPIC_1, producer);

			extracted(random, KafkaConfig.INPUT_TOPIC_2, producer);

			sleep(300);
		}

		// flush data - synchronous
		producer.flush();

		// flush and close producer
		producer.close();
	}

	private static void extracted(final Random random, final String inputTopic1, final KafkaProducer<String, String> producer) {
		final int randomNumber = random.nextInt(3);
		// create a producer record
		final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(inputTopic1, "cogroup_" + randomNumber, "tag_" + randomNumber);
		// send data - asynchronous
		producer.send(producerRecord);
	}
}
