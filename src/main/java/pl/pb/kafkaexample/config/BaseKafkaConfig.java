package pl.pb.kafkaexample.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.UUID;

public class BaseKafkaConfig {

	private static final String URL = "localhost:9092";
	public static final String APPLICATION_ID = "streams-example";
	public static final String GROUP_ID = "example-consumer-group-id";
	public static final String AUTO_OFFSET_RESET = "earliest";
	public static final String ENABLE_AUTO_COMMIT = "false";

	private BaseKafkaConfig() {
	}

	public static Properties getStreamsConfig() {
		final Properties props = new Properties();
		props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
		props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, URL);
		props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.putIfAbsent(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
		return props;
	}

	public static Properties getConsumerConfig() {
		final Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, URL);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID + UUID.randomUUID().toString());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT);
		return properties;
	}

	public static Properties getProducerConfig() {
		final Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, URL);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}
}