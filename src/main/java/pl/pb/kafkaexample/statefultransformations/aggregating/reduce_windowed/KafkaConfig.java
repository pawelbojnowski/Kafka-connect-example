package pl.pb.kafkaexample.statefultransformations.aggregating.reduce_windowed;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsConfig;
import pl.pb.kafkaexample.config.BaseKafkaConfig;

import java.util.Properties;

public class KafkaConfig {

	static final String INPUT_TOPIC_1 = "kafka_example_input_1";
	static final String OUTPUT_TOPIC_1 = "kafka_example_output_1";
	static final String OUTPUT_TOPIC_2 = "kafka_example_output_2";
	static final String OUTPUT_TOPIC_3 = "kafka_example_output_3";

	private KafkaConfig() {
	}

	static Properties getStreamsConfig() {
		final Properties props = BaseKafkaConfig.getStreamsConfig();
		props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		return props;
	}

	static Properties getConsumerConfig() {
		final Properties properties = BaseKafkaConfig.getConsumerConfig();
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		return properties;
	}

	static Properties getProducerConfig() {
		final Properties properties = BaseKafkaConfig.getProducerConfig();
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		return properties;
	}
}
