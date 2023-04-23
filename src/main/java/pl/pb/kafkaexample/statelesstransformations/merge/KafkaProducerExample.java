package pl.pb.kafkaexample.statelesstransformations.merge;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerExample {

	public static void main(final String[] args) {

		// create the producer
		final KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProducerConfig());

		// create a producer consumerRecord for stream 1
		final ProducerRecord<String, String> producerRecord1 = new ProducerRecord<>(KafkaConfig.INPUT_TOPIC_1, "selectKeyExample", "hello world value 1");

		// send data - asynchronous for stream 1
		producer.send(producerRecord1);

		// create a producer consumerRecord for stream 2
		final ProducerRecord<String, String> producerRecord2 = new ProducerRecord<>(KafkaConfig.INPUT_TOPIC_2, "selectKeyExample", "hello world value 2");

		// send data - asynchronous for stream 2
		producer.send(producerRecord2);

		// flush data - synchronous
		producer.flush();

		// flush and close producer
		producer.close();
	}
}
