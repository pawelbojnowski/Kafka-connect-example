package pl.pb.kafkaexample.statelesstransformations.flatmapvalue;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.List;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statelesstransformations.flatmapvalue.KafkaConfig.INPUT_TOPIC_1;
import static pl.pb.kafkaexample.statelesstransformations.flatmapvalue.KafkaConfig.getStreamsConfig;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {
		final KStream<String, String> stream = builder.stream(INPUT_TOPIC_1);
		stream.flatMapValues(
				// Here, we generate two output records for each input consumerRecord.
				// We also change the key and value types.
				// Example: ("flatMapExample", "flatMapExample") -> ("HELLO WORLD VALUE", "1000"), (" hello world value", "9000")
				value -> {
					println("VALUE: " + value);
					return List.of(value.toUpperCase(), value.toLowerCase());
				}
		).to(KafkaConfig.OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.String()));
	}
}
