package pl.pb.kafkaexample.statelesstransformations.filternot_inversefilter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static pl.pb.kafkaexample.statelesstransformations.filternot_inversefilter.KafkaConfig.getProducerConfig;

public class KafkaProducerExample {

	public static void main(final String[] args) {

		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerConfig());

		// create a producer record
		final ProducerRecord<String, String> producerRecord1 = new ProducerRecord<>(KafkaConfig.INPUT_TOPIC_1, "filterExample", "hello world value");
		final ProducerRecord<String, String> producerRecord2 = new ProducerRecord<>(KafkaConfig.INPUT_TOPIC_1, "selectKeyExample", "world value");

		// send data - asynchronous
		producer.send(producerRecord1);
		producer.send(producerRecord2);

		// flush data - synchronous
		producer.flush();

		// flush and close producer
		producer.close();
	}
}
