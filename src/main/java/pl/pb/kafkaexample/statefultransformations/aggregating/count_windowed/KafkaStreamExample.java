package pl.pb.kafkaexample.statefultransformations.aggregating.count_windowed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statefultransformations.aggregating.count_windowed.KafkaConfig.*;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {
		final KStream<String, String> source = builder.stream(INPUT_TOPIC_1);

		kGroupedStreamWithTimeWindows(source);
		kGroupedStreamWithSlidingWindows(source);
		kGroupedStreamWithSessionWindows(source);
	}

	private static void kGroupedStreamWithTimeWindows(final KStream<String, String> source) {

		/** Aggregating with time-based windowing (here: with 1-second tumbling windows) */
		final TimeWindows windows = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1));

		final KGroupedStream<String, String> groupedStream = source.groupByKey();

		final KTable<Windowed<String>, Long> aggregatedStream = groupedStream
				.windowedBy(windows)
				.count();

		aggregatedStream.toStream()
				.peek((key, value) -> println(key.key() + " - " + value))
				.map((keyWindowed, value) -> KeyValue.pair(keyWindowed.key(), value))
				.to(OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.Long()));
	}

	private static void kGroupedStreamWithSlidingWindows(final KStream<String, String> source) {

		/** Counting a KGroupedStream windowing (here: with 1-minute sliding windows and 2-minute grace period) */
		final SlidingWindows windows = SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(1), Duration.ofMillis(2));

		final KGroupedStream<String, String> groupedStream = source.groupByKey();

		final KTable<Windowed<String>, Long> aggregatedStream = groupedStream
				.windowedBy(windows)
				.count();

		aggregatedStream.toStream()
				.peek((key, value) -> println(key.key() + " - " + value))
				.map((keyWindowed, value) -> KeyValue.pair(keyWindowed.key(), value))
				.to(OUTPUT_TOPIC_2, Produced.with(Serdes.String(), Serdes.Long()));
	}

	private static void kGroupedStreamWithSessionWindows(final KStream<String, String> source) {

		/** Counting a KGroupedStream windowing (here: with an inactivity gap of 100 millis) */
		final SessionWindows windows = SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMillis(100));

		final KGroupedStream<String, String> groupedStream = source.groupByKey();

		final KTable<Windowed<String>, Long> aggregatedStream = groupedStream
				.windowedBy(windows)
				.count();

		aggregatedStream.toStream()
				.peek((key, value) -> println(key.key() + " - " + value))
				.map((keyWindowed, value) -> KeyValue.pair(keyWindowed.key(), value))
				.to(OUTPUT_TOPIC_3, Produced.with(Serdes.String(), Serdes.Long()));
	}
}
