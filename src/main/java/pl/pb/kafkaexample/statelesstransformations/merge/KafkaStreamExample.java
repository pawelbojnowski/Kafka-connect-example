package pl.pb.kafkaexample.statelesstransformations.merge;

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

		// S T R E A M - 1
		final KStream<String, String> stream1 = builder.stream(KafkaConfig.INPUT_TOPIC_1);

		// S T R E A M - 2
		final KStream<String, String> stream2 = builder.stream(KafkaConfig.INPUT_TOPIC_2);

		final KStream<String, String> allMessages = stream1.merge(stream2);

		allMessages.to(KafkaConfig.OUTPUT_TOPIC_1);
	}
}
