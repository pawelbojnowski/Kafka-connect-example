package pl.pb.kafkaconnectexample.proto;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pl.pb.kafkamodel.user.Client;

import java.util.UUID;

import static pl.pb.kafkaconnectexample.config.Commons.println;
import static pl.pb.kafkaconnectexample.proto.KafkaConfig.POSTGRES_SINK_CLIENT;
import static pl.pb.kafkaconnectexample.proto.KafkaConfig.getProducer;

public class KafkaProducerExample {


	public static void main(final String[] args) {
//		KafkaProtobufSerializer
		// create the producer
		final KafkaProducer<String, Object> producer = getProducer();

		final	Client client = Client.newBuilder()
				.setClientId(UUID.randomUUID().toString())
				.setId(10)
				.setFirstname("Tom")
				.setLastname("Good")
				.setPhoneNumber(12312312)
				.build();


		send(POSTGRES_SINK_CLIENT, producer, null, client);

		// flush data - synchronous
		producer.flush();

		// flush and close producer
		producer.close();
	}

	private static void send(final String inputTopic, final KafkaProducer<String, Object> producer, final String key, final Object value) {

		// create a producer record
		final ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(inputTopic, key, value);

		// send data - asynchronous
		producer.send(producerRecord);

		println("Sent message for 'recordMessages': " + producerRecord);
	}
}

