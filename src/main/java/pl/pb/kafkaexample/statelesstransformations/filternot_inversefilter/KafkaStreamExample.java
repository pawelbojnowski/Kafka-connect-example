package pl.pb.kafkaexample.statelesstransformations.filternot_inversefilter;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.statelesstransformations.filternot_inversefilter.KafkaConfig.*;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {

		final KStream<String, String> stream = builder.stream(INPUT_TOPIC_1);

		stream
				.filter((key, value) -> value.contains("hello"))
				.to(OUTPUT_TOPIC_1);
	}
}
