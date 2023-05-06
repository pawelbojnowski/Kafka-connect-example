package pl.pb.kafkaconnectexample.postgress.jsonschema;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static pl.pb.kafkaconnectexample.postgress.config.Commons.println;
import static pl.pb.kafkaconnectexample.postgress.jsonschema.KafkaConfig.POSTGRES_SINK_CLIENT;
import static pl.pb.kafkaconnectexample.postgress.jsonschema.KafkaConfig.getProducer;

public class KafkaProducerExample {


	public static void main(final String[] args) {

		// create the producer
		final KafkaProducer<String, String> producer = getProducer();

		send(POSTGRES_SINK_CLIENT, producer, null, "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"firstname\"},{\"type\":\"string\",\"optional\":true,\"field\":\"lastname\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"phone_number\"}],\"optional\":false,\"name\":\"user\"}," +
												   "\"payload\":{\"id\":1,\"firstname\":\"Jack\",\"lastname\":\"Sparrow\",\"phone_number\":200200200}},");

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
