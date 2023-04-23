package pl.pb.kafkaexample.statelesstransformations.filter;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), KafkaConfig.getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {

		final KStream<String, String> stream = builder.stream(KafkaConfig.INPUT_TOPIC_1);

		stream
				.filter((key, value) -> value.contains("hello"))
				.to(KafkaConfig.OUTPUT_TOPIC_1);
	}
}
