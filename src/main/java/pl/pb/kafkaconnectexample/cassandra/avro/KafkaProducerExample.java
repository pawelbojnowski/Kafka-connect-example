package pl.pb.kafkaconnectexample.cassandra.avro;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pl.pb.kafkamodel.avro.User;

import static pl.pb.kafkaconnectexample.cassandra.avro.KafkaConfig.CASSANDRA_SINK_CLIENT;
import static pl.pb.kafkaconnectexample.cassandra.config.Commons.println;

public class KafkaProducerExample {


	public static void main(final String[] args) {

		// create the producer
		final KafkaProducer<Integer, User> producer = KafkaConfig.<Integer, User>getProducer();
		send(CASSANDRA_SINK_CLIENT, producer, "042dfae2-750a-4da5-8c90-f1397f09664f", User.newBuilder()
				.setId(4)
				.setFirstname("Harry")
				.setLastname("Potter")
				.setPhoneNumber(400400400)
				.build());

		// flush data - synchronous
		producer.flush();

		// flush and close producer
		producer.close();
	}

	private static void send(final String inputTopic, final KafkaProducer producer, final String key, final User value) {

		// create a producer record
		final ProducerRecord<String, User> producerRecord = new ProducerRecord<>(inputTopic, key, value);

		// send data - asynchronous
		producer.send(producerRecord);

		println("Sent message for 'recordMessages': " + producerRecord);
	}
}

