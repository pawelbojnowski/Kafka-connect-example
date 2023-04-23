package pl.pb.kafkaexample.statelesstransformations.flatmap;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.LinkedList;
import java.util.List;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), KafkaConfig.getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {
		final KStream<String, String> stream = builder.stream(KafkaConfig.INPUT_TOPIC_1);
		stream.flatMap(
				// Here, we generate two output records for each input consumerRecord.
				// We also change the key and value types.
				// Example: ("flatMapExample", "flatMapExample") -> ("HELLO WORLD VALUE", "1000"), (" hello world value", "9000")
				(key, value) -> {
					println("KEY: " + key + ",  VALUE: " + value);
					final List<KeyValue<String, String>> result = new LinkedList<>();
					result.add(KeyValue.pair(value.toUpperCase(), "1000"));
					result.add(KeyValue.pair(value.toLowerCase(), "9000"));
					return result;
				}
		).to(KafkaConfig.OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.String()));
	}
}
