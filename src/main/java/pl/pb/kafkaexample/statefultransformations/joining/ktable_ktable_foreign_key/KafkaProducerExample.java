package pl.pb.kafkaexample.statefultransformations.joining.ktable_ktable_foreign_key;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.config.Commons.sleep;
import static pl.pb.kafkaexample.statefultransformations.joining.ktable_ktable_foreign_key.KafkaConfig.getProducerConfig;

public class KafkaProducerExample {

	public static void main(final String[] args) {

		// create the producer
		final KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerConfig());

		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, "k", "1");
		send(KafkaConfig.INPUT_TOPIC_RIGHT, producer, "1", "foo");
		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, "k", "2");
		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, "k", "3");
		send(KafkaConfig.INPUT_TOPIC_RIGHT, producer, "2", "bar");
		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, "k", null);
		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, "k", "1");
		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, "q", "1");
		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, "r", "1");
		send(KafkaConfig.INPUT_TOPIC_RIGHT, producer, "10", "baz");

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

		sleep(500);
	}
}
