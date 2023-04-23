package pl.pb.kafkaexample.processor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static pl.pb.kafkaexample.config.Commons.println;

public class KafkaProducerExample {


	public static void main(final String[] args) throws IOException {

		// create the producer
		final KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProducerConfig());

		final Path path = Paths.get("src/main/resources/Kafka_-_What_is_event_streaming.txt");

		Files.readAllLines(path)
				.forEach(text -> send(KafkaConfig.INPUT_TOPIC_1, producer, "TheProcessorAPI", text));

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
