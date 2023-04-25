package pl.pb.kafkaconnectexample.jsonschema;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import static pl.pb.kafkaconnectexample.config.Commons.println;
import static pl.pb.kafkaconnectexample.jsonschema.KafkaConfig.POSTGRES_SOURCED_USER;
import static pl.pb.kafkaconnectexample.jsonschema.KafkaConfig.runStreams;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();
		createStream(builder);
		runStreams(builder.build());

	}

	static void createStream(final StreamsBuilder builder) {
		final KStream<String, String> source = builder.stream(POSTGRES_SOURCED_USER);
		source.peek((key, value) -> println(key + " = " + value))
				.to(KafkaConfig.POSTGRES_SINK_CLIENT, Produced.with(Serdes.String(), Serdes.String()));
	}

}
