package pl.pb.kafkaconnectexample.cassandra.proto;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaConfigProperties {

	public static final String URL = "localhost:9091";

	private KafkaConfigProperties() {
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