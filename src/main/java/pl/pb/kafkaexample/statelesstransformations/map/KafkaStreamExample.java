package pl.pb.kafkaexample.statelesstransformations.map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.statelesstransformations.map.KafkaConfig.INPUT_TOPIC_1;
import static pl.pb.kafkaexample.statelesstransformations.map.KafkaConfig.getStreamsConfig;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {
		final KStream<String, String> stream = builder.stream(INPUT_TOPIC_1);
		stream.map((key, value) -> KeyValue.pair("New_" + key, value.toUpperCase()))
				.to(KafkaConfig.OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.String()));
	}
}
