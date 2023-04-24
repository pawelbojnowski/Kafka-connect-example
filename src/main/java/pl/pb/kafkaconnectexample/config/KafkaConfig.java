package pl.pb.kafkaconnectexample.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class KafkaConfig {

	public static final String POSTGRES_SOURCED_USER = "postgres.connector.source.user";
	public static final String POSTGRES_SOURCED_USER_SINK = "postgres.connector.sink.user";
	private static final String URL = "https://localhost:9091";
	private static final String GROUP_ID = "kafka-connect-example-group-id";
	private static final String AUTO_OFFSET_RESET = "earliest";
	private static final String ENABLE_AUTO_COMMIT = "false";

	private KafkaConfig() {
	}


	public static KafkaConsumer<String, RecordBatch> getConsumer(final String postgresSourcedTestDb) {
		KafkaConsumer<String, RecordBatch> consumer = new KafkaConsumer<>(getConsumerConfig());

		// add subscribed topic(s)
		consumer.subscribe(List.of(postgresSourcedTestDb));

		return consumer;
	}

	public static KafkaProducer<String, String> getProducer() {
		return new KafkaProducer<>(KafkaConfig.getProducerConfig());
	}

	private static Properties getConsumerConfig() {
		final Properties properties = getProperties();
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		return properties;
	}

	private static Properties getProperties() {
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
