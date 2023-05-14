package pl.pb.kafkaconnectexample.cassandra.avro;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import pl.pb.kafkamodel.avro.Client;
import pl.pb.kafkamodel.avro.User;

import static pl.pb.kafkaconnectexample.cassandra.avro.KafkaConfig.CASSANDRA_SOURCED_USER;
import static pl.pb.kafkaconnectexample.cassandra.avro.KafkaConfig.runStreams;
import static pl.pb.kafkaconnectexample.cassandra.config.Commons.println;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();
		createStream(builder);
		runStreams(builder.build());
	}

	private static void createStream(final StreamsBuilder builder) {
		final KStream<String, User> source = builder.stream(CASSANDRA_SOURCED_USER);
		source.peek((key, value) -> println(key + " = " + value))
				.mapValues(value -> mapUserToClient(value))
				.to(KafkaConfig.CASSANDRA_SINK_CLIENT);
	}

	private static Client mapUserToClient(User value) {
		return Client.newBuilder()
				.setId(value.getId())
				.setFirstname(value.getFirstname())
				.setLastname(value.getLastname())
				.setPhoneNumber(value.getPhoneNumber())
				.build();
	}


}
