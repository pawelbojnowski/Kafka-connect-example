package pl.pb.kafkaexample.ktable;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static pl.pb.kafkaexample.ktable.KafkaConfig.INPUT_TOPIC_1;
import static pl.pb.kafkaexample.ktable.KafkaConfig.getProducerConfig;

public class KafkaProducerExample {

	public static void main(final String[] args) {

		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerConfig());

		// create a producer record
		final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(INPUT_TOPIC_1, "stream-value", "hello world from kafka in Java");

		// send data - asynchronous
		producer.send(producerRecord);

		// flush data - synchronous
		producer.flush();

		// flush and close producer
		producer.close();
	}
}
