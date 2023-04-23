package pl.pb.kafkaexample.statefultransformations.joining.kstream_kstream;

import pl.pb.kafkaexample.config.BaseKafkaConfig;

import java.util.Properties;

public class KafkaConfig {

	static final String INPUT_TOPIC_LEFT = "kafka_example_input_1";
	static final String INPUT_TOPIC_RIGHT = "kafka_example_input_2";
	static final String OUTPUT_TOPIC_1 = "kafka_example_output_1";
	static final String OUTPUT_TOPIC_2 = "kafka_example_output_2";
	static final String OUTPUT_TOPIC_3 = "kafka_example_output_3";

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
