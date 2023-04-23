package pl.pb.kafkaexample.kstream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerExample {

	public static void main(final String[] args) {

		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProducerConfig());

		// create a producer record
		final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(KafkaConfig.INPUT_TOPIC_1, "baseExample", "hello world value");

		// send data - asynchronous
		producer.send(producerRecord);

		// flush data - synchronous
		producer.flush();

		// flush and close producer
		producer.close();
	}
}
