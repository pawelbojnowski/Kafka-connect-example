package pl.pb.kafkaconnectexample.cassandra.proto;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import pl.pb.kafkamodel.proto.User;

import java.util.Properties;
import java.util.UUID;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE;

public class KafkaConfigProperties {

	public static final String URL = "localhost:9091";
	public static final String APPLICATION_ID = "streams-example";
	public static final String GROUP_ID = "example-consumer-group-id";
	public static final String AUTO_OFFSET_RESET = "earliest";
	public static final String ENABLE_AUTO_COMMIT = "false";

	private KafkaConfigProperties() {
	}

	public static Properties getStreamsConfig() {

		final Properties properties = new Properties();
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, URL);
		properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaProtobufSerde.class.getName());
		properties.setProperty(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1");
		properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		properties.setProperty("enable.auto.commit", "false");
		properties.setProperty(SPECIFIC_PROTOBUF_VALUE_TYPE, User.class.getName());
		return properties;
	}

	public static Properties getConsumerConfig() {
		final Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, URL);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID + UUID.randomUUID());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT);
		properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		properties.setProperty(SPECIFIC_PROTOBUF_VALUE_TYPE, User.class.getName());
		return properties;
	}

	public static Properties getProducerConfig() {

		final Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, URL);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());
		properties.put("schema.registry.url", "http://127.0.0.1:8081");
		return properties;
	}
}