package pl.pb.kafkaexample.statefultransformations.aggregating.reduce;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statefultransformations.aggregating.reduce.KafkaConfig.*;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {
		final KStream<String, Long> source = builder.stream(INPUT_TOPIC_1);

		kGroupedStream(source);
		kGroupedTable(source);
	}

	private static void kGroupedStream(final KStream<String, Long> source) {
		final KGroupedStream<String, Long> groupedStream = source.groupByKey();

		final KTable<String, Long> aggregatedStream = groupedStream.reduce(
				(aggValue, newValue) -> aggValue + newValue /* adder */);

		aggregatedStream.toStream()
				.peek((key, value) -> println(key + " - " + value))
				.to(OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.Long()));
	}

	private static void kGroupedTable(final KStream<String, Long> source) {
		final KGroupedTable<String, Long> groupedTable = source.toTable().groupBy(KeyValue::pair);

		final KTable<String, Long> aggregatedStream = groupedTable.reduce(
				(aggValue, newValue) -> aggValue + newValue, /* adder */
				(aggValue, oldValue) -> aggValue - oldValue /* subtractor */
		);

		aggregatedStream.toStream()
				.peek((key, value) -> println(key + " - " + value))
				.to(OUTPUT_TOPIC_2, Produced.with(Serdes.String(), Serdes.Long()));
	}
}
