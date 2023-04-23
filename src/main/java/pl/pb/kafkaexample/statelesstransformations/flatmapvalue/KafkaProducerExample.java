package pl.pb.kafkaexample.statelesstransformations.flatmapvalue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static pl.pb.kafkaexample.statelesstransformations.flatmapvalue.KafkaConfig.getProducerConfig;

public class KafkaProducerExample {

	public static void main(final String[] args) {

		// create the producer
		final KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerConfig());

		// create a producer record
		final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(KafkaConfig.INPUT_TOPIC_1, "flatMapValueExample", "hello world value");

		// send data - asynchronous
		producer.send(producerRecord);

		// flush data - synchronous
		producer.flush();

		// flush and close producer
		producer.close();
	}
}
