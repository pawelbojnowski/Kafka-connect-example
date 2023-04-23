package pl.pb.kafkaexample.statefultransformations.aggregating.aggregate;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statefultransformations.aggregating.aggregate.KafkaConfig.*;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {
		final KStream<String, String> source = builder.stream(INPUT_TOPIC_1);

		kGroupedStream(source);
		kGroupedTable(source);
	}

	private static void kGroupedStream(final KStream<String, String> source) {
		final KGroupedStream<String, String> groupedStream = source.groupByKey();
		final Initializer<Long> initializer = () -> 0L;
		final Aggregator<String, String, Long> aggregator = (aggKey, newValue, aggValue) -> aggValue + newValue.length();
		final Materialized<String, Long, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store-KGroupedStream")
				.withValueSerde(Serdes.Long());

		final KTable<String, Long> aggregatedStream = groupedStream.aggregate(
				initializer, /* initializer */
				aggregator, /* adder */
				materialized /*Materialized*/
		);

		aggregatedStream.toStream()
				.peek((key, value) -> println(key + " - " + value))
				.to(OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.Long()));
	}

	private static void kGroupedTable(final KStream<String, String> source) {
		final KGroupedTable<String, String> groupedTable = source.toTable().groupBy(KeyValue::pair);
		final Initializer<Long> initializer = () -> 0L;
		final Aggregator<String, String, Long> aggregator = (aggKey, newValue, aggValue) -> aggValue + newValue.length();
		final Materialized<String, Long, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store-KGroupedTable")
				.withValueSerde(Serdes.Long());

		final KTable<String, Long> aggregatedStream = groupedTable.aggregate(
				initializer, /* initializer */
				aggregator, /* adder */
				aggregator, /* adder */
				materialized /*Materialized*/
		);

		aggregatedStream.toStream()
				.peek((key, value) -> println(key + " - " + value))
				.to(OUTPUT_TOPIC_2, Produced.with(Serdes.String(), Serdes.Long()));
	}
}
