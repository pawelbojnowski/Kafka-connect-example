package pl.pb.kafkaexample.globalktable;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static pl.pb.kafkaexample.globalktable.KafkaConfig.*;

public class KafkaProducerExample {

	public static void main(final String[] args) {

		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerConfig());

		// create a producer record
		final ProducerRecord<String, String> producerRecord1 = new ProducerRecord<>(INPUT_TOPIC_1, "join-stream-value", "hello world from");
		final ProducerRecord<String, String> producerRecord2 = new ProducerRecord<>(INPUT_TOPIC_2, "join-stream-value", "kafka in Java");

		// send data - asynchronous
		producer.send(producerRecord1);
		producer.send(producerRecord2);

		// flush data - synchronous
		producer.flush();

		// flush and close producer
		producer.close();
	}
}
