package pl.pb.kafkaconnectexample.proto;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pl.pb.kafkamodel.user.User;

import static pl.pb.kafkaconnectexample.config.Commons.println;
import static pl.pb.kafkaconnectexample.proto.KafkaConfig.POSTGRES_SINK_USER_UPDATE;

public class KafkaProducerUpdateUserExample {


	public static void main(final String[] args) {

		// create the producer
		final KafkaProducer<Integer, User> producer = KafkaConfig.<Integer, User>getProducer();
		send(POSTGRES_SINK_USER_UPDATE, producer, null, User.newBuilder()
				.setId(3)
				.setFirstname("Brian")
				.setLastname("O’Conner")
				.setPhoneNumber(333333333)
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

