package pl.pb.kafkaconnectexample.proto;

import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import pl.pb.kafkamodel.proto.Client;
import pl.pb.kafkamodel.proto.User;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE;
import static pl.pb.kafkaconnectexample.config.Commons.println;
import static pl.pb.kafkaconnectexample.proto.KafkaConfig.POSTGRES_SOURCED_USER;
import static pl.pb.kafkaconnectexample.proto.KafkaConfig.runStreams;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();
		createStream(builder);
		runStreams(builder.build());
	}

	private static void createStream(final StreamsBuilder builder) {

		Consumed<String, User> consumed = Consumed.with(Serdes.String(), userProtobufSerde());
		Produced<String, Client> produced = Produced.with(Serdes.String(), clientProtobufSerde());

		final KStream<String, User> source = builder.stream(POSTGRES_SOURCED_USER, consumed);
		source.peek((key, value) -> println(key + " = " + value))
				.mapValues(value -> mapUserToClient(value))
				.to(KafkaConfig.POSTGRES_SINK_CLIENT, produced);
	}

	private static KafkaProtobufSerde<User> userProtobufSerde() {
		final KafkaProtobufSerde<User> protobufSerde = new KafkaProtobufSerde();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		serdeConfig.put(SPECIFIC_PROTOBUF_VALUE_TYPE, User.class.getName());
		protobufSerde.configure(serdeConfig, false);
		return protobufSerde;
	}

	private static KafkaProtobufSerde<Client> clientProtobufSerde() {

//		Client.parseFrom(new ByteBuffer());

		final KafkaProtobufSerde<Client> protobufSerde = new KafkaProtobufSerde();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		serdeConfig.put(SPECIFIC_PROTOBUF_VALUE_TYPE, Client.class.getName());
		protobufSerde.configure(serdeConfig, false);
		return protobufSerde;
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
