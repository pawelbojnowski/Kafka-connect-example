package pl.pb.kafkaexample.statelesstransformations.selectkey;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.statelesstransformations.selectkey.KafkaConfig.INPUT_TOPIC_1;
import static pl.pb.kafkaexample.statelesstransformations.selectkey.KafkaConfig.getStreamsConfig;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {
		final KStream<String, String> stream = builder.stream(INPUT_TOPIC_1);
		stream.selectKey((key, value) -> key.toUpperCase())
				.to(KafkaConfig.OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.String()));
	}

}
