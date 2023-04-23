package pl.pb.kafkaexample.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.ktable.KafkaConfig.*;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {

		//KStream
		final KStream<String, String> kStream = builder.stream(INPUT_TOPIC_1);

		//Join KStream with GlobalKTable
		final KTable<String, String> kTable = kStream
				.peek((key, value) -> println("Processed message: " + key + " = " + value))
				.mapValues(value -> value.toUpperCase())
				.toTable();

		//Forward result
		kTable.toStream().to(OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.String()));
	}
}
