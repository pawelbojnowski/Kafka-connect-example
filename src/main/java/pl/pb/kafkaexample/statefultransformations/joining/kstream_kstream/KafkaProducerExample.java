package pl.pb.kafkaexample.statefultransformations.joining.kstream_kstream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.config.Commons.sleep;
import static pl.pb.kafkaexample.statefultransformations.joining.kstream_kstream.KafkaConfig.getProducerConfig;

public class KafkaProducerExample {

	public static void main(final String[] args) {

		// create the producer
		final KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerConfig());

		final String key = "joiningKafka";
		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, key, null);
		send(KafkaConfig.INPUT_TOPIC_RIGHT, producer, key, null);

		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, key, "A");
		send(KafkaConfig.INPUT_TOPIC_RIGHT, producer, key, "a");

		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, key, "B");
		send(KafkaConfig.INPUT_TOPIC_RIGHT, producer, key, "b");

		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, key, null);
		send(KafkaConfig.INPUT_TOPIC_RIGHT, producer, key, null);

		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, key, "C");
		send(KafkaConfig.INPUT_TOPIC_RIGHT, producer, key, "c");

		send(KafkaConfig.INPUT_TOPIC_RIGHT, producer, key, null);
		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, key, null);
		send(KafkaConfig.INPUT_TOPIC_RIGHT, producer, key, null);

		send(KafkaConfig.INPUT_TOPIC_RIGHT, producer, key, "d");
		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, key, "D");
		sleep(4000);
		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, key, "E");
		sleep(4000);
		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, key, "F");
		sleep(4000);
		send(KafkaConfig.INPUT_TOPIC_RIGHT, producer, key, "f");
		sleep(4000);
		send(KafkaConfig.INPUT_TOPIC_LEFT, producer, key, "G");

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
