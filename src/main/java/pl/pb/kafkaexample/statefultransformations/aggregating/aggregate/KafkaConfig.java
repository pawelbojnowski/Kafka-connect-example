package pl.pb.kafkaexample.statefultransformations.aggregating.aggregate;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import pl.pb.kafkaexample.config.BaseKafkaConfig;

import java.util.Properties;

public class KafkaConfig {

	static final String INPUT_TOPIC_1 = "kafka_example_input_1";
	static final String OUTPUT_TOPIC_1 = "kafka_example_output_1";
	static final String OUTPUT_TOPIC_2 = "kafka_example_output_2";

	private KafkaConfig() {
	}

	static Properties getStreamsConfig() {
		return BaseKafkaConfig.getStreamsConfig();
	}

	static Properties getConsumerConfig() {
		final Properties properties = BaseKafkaConfig.getConsumerConfig();
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		return properties;
	}

	static Properties getProducerConfig() {
		return BaseKafkaConfig.getProducerConfig();
	}
}
