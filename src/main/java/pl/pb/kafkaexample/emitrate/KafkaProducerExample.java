package pl.pb.kafkaexample.emitrate;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

import static pl.pb.kafkaexample.config.Commons.println;

public class KafkaProducerExample {


	public static void main(final String[] args) {

		// create the producer
		final KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProducerConfig());

		for (int i = 0; i < 100; i++) {
			final int id = new Random().nextInt(4);
			send(KafkaConfig.INPUT_TOPIC_1, producer, "key" + id, "Message for key with id: " + id);

		}

		// flush data - synchronous
		producer.flush();

		// flush and close producer
		producer.close();
	}

	private static void send(final String inputTopic, final KafkaProducer<String, String> producer, final String key, final String value) {

		// create a producer record
		final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(inputTopic, key, value);

		// send data - asynchronous
		producer.send(producerRecord);

		println("Sent message for 'recordMessages': " + producerRecord);
	}
}
