package pl.pb.kafkaexample.ktable;

import pl.pb.kafkaexample.config.BaseKafkaConfig;

import java.util.Properties;

public class KafkaConfig {

	static final String INPUT_TOPIC_1 = "kafka_example_input_1";
	static final String OUTPUT_TOPIC_1 = "kafka_example_output_1";

	private KafkaConfig() {
	}

	static Properties getStreamsConfig() {
		return BaseKafkaConfig.getStreamsConfig();
	}

	static Properties getConsumerConfig() {
		return BaseKafkaConfig.getConsumerConfig();
	}

	static Properties getProducerConfig() {
		return BaseKafkaConfig.getProducerConfig();
	}
}
