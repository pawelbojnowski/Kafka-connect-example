package pl.pb.kafkaconnectexample.cassandra.proto;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pl.pb.kafkamodel.proto.User;

import static pl.pb.kafkaconnectexample.cassandra.config.Commons.println;
import static pl.pb.kafkaconnectexample.cassandra.proto.KafkaConfig.CASSANDRA_SINK_USER_INSERT;

public class KafkaProducerInsertUserExample {


	public static void main(final String[] args) {

		// create the producer
		final KafkaProducer<Integer, User> producer = KafkaConfig.<Integer, User>getProducer();
		send(CASSANDRA_SINK_USER_INSERT, producer, null, User.newBuilder()
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

	private static void send(final String inputTopic, final KafkaProducer producer, final Integer key, final User value) {

		// create a producer record
		final ProducerRecord<Integer, User> producerRecord = new ProducerRecord<>(inputTopic, key, value);

		// send data - asynchronous
		producer.send(producerRecord);

		println("Sent message for 'recordMessages': " + producerRecord);
	}
}
