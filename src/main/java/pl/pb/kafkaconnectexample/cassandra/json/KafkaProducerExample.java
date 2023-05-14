package pl.pb.kafkaconnectexample.cassandra.json;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.UUID;

import static pl.pb.kafkaconnectexample.cassandra.config.Commons.println;
import static pl.pb.kafkaconnectexample.cassandra.json.KafkaConfig.CASSANDRA_SINK_CLIENT;
import static pl.pb.kafkaconnectexample.cassandra.json.KafkaConfig.getProducer;

public class KafkaProducerExample {


	public static void main(final String[] args) {

		// create the producer
		final KafkaProducer<String, String> producer = getProducer();

		send(CASSANDRA_SINK_CLIENT, producer, UUID.randomUUID().toString(), "{\n" +
																			"  \"id\": 1,\n" +
																			"  \"firstname\": \"Jack\",\n" +
																			"  \"lastname\": \"Sparrow\",\n" +
																			"  \"phone_number\": 200200200\n" +
																			"}");

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
