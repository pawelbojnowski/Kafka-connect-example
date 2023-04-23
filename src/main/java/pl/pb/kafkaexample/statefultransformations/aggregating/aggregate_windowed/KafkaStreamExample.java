package pl.pb.kafkaexample.statefultransformations.aggregating.aggregate_windowed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

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
		final KStream<String, String> source = builder.stream(KafkaConfig.INPUT_TOPIC_1);

		kGroupedStreamWithTimeWindows(source);
		kGroupedStreamWithSlidingWindows(source);
		kGroupedStreamWithSessionWindows(source);
	}

	private static void kGroupedStreamWithTimeWindows(final KStream<String, String> source) {

		/** Aggregating with time-based windowing (here: with 1-second tumbling windows) */
		final TimeWindows windows = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1));

		final KGroupedStream<String, String> groupedStream = source.groupByKey();
		final Initializer<Long> initializer = () -> 0L;
		final Aggregator<String, String, Long> aggregator = (aggKey, newValue, aggValue) -> aggValue + 1;
		final Materialized<String, Long, WindowStore<Bytes, byte[]>> materialized = Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("aggregated-stream-store-KGroupedStream")
				.withValueSerde(Serdes.Long());

		final KTable<Windowed<String>, Long> aggregatedStream = groupedStream
				.windowedBy(windows)
				.aggregate(
						initializer,
						aggregator,
						materialized
				);

		aggregatedStream.toStream()
				.peek((key, value) -> println(key.key() + " - " + value))
				.map((keyWindowed, value) -> KeyValue.pair(keyWindowed.key(), value))
				.to(KafkaConfig.OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.Long()));
	}

	private static void kGroupedStreamWithSlidingWindows(final KStream<String, String> source) {

		/** Aggregating with time-based windowing (here: with 1-minute sliding windows and 2-minute grace period) */
		final SlidingWindows windows = SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(1), Duration.ofMillis(2));

		final KGroupedStream<String, String> groupedStream = source.groupByKey();
		final Initializer<Long> initializer = () -> 0L;
		final Aggregator<String, String, Long> aggregator = (aggKey, newValue, aggValue) -> aggValue + 1;
		final Materialized<String, Long, WindowStore<Bytes, byte[]>> materialized = Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("aggregated-stream-store-KGroupedStreamWithSlidingWindows")
				.withValueSerde(Serdes.Long());

		final KTable<Windowed<String>, Long> aggregatedStream = groupedStream
				.windowedBy(windows)
				.aggregate(
						initializer,
						aggregator,
						materialized
				);

		aggregatedStream.toStream()
				.peek((key, value) -> println(key.key() + " - " + value))
				.map((keyWindowed, value) -> KeyValue.pair(keyWindowed.key(), value))
				.to(KafkaConfig.OUTPUT_TOPIC_2, Produced.with(Serdes.String(), Serdes.Long()));
	}

	private static void kGroupedStreamWithSessionWindows(final KStream<String, String> source) {

		/** Aggregating with session-based windowing (here: with an inactivity gap of 100 milli) */
		final SessionWindows windows = SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMillis(100));

		final KGroupedStream<String, String> groupedStream = source.groupByKey();
		final Initializer<Long> initializer = () -> 0L;
		final Aggregator<String, String, Long> aggregator = (aggKey, newValue, aggValue) -> aggValue + 1;
		final Materialized<String, Long, SessionStore<Bytes, byte[]>> materialized = Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("aggregated-stream-store-KGroupedStreamWithSessionWindows")
				.withValueSerde(Serdes.Long());
		final Merger<String, Long> stringLongMerger = (key, leftValue, rightValue) -> leftValue + rightValue;

		final KTable<Windowed<String>, Long> aggregatedStream = groupedStream
				.windowedBy(windows)
				.aggregate(
						initializer,
						aggregator,
						stringLongMerger,
						materialized
				);

		aggregatedStream.toStream()
				.peek((key, value) -> println(key.key() + " - " + value))
				.map((keyWindowed, value) -> KeyValue.pair(keyWindowed.key(), value))
				.to(KafkaConfig.OUTPUT_TOPIC_3, Produced.with(Serdes.String(), Serdes.Long()));
	}
}
