package pl.pb.kafkaexample.statefultransformations.windowing.sessionwindows;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statefultransformations.windowing.sessionwindows.KafkaConfig.OUTPUT_TOPIC_1;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), KafkaConfig.getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {
		final KStream<String, String> source = builder.stream(KafkaConfig.INPUT_TOPIC_1);

		kGroupedTable(source);
	}


	private static void kGroupedTable(final KStream<String, String> source) {

		final Duration inactivityGap = Duration.ofSeconds(2);
		final SessionWindows sessionWindows = SessionWindows.ofInactivityGapWithNoGrace(inactivityGap);

		final KTable<Windowed<String>, Long> aggregatedStream = source.groupByKey()
				.windowedBy(sessionWindows)
				.count();

		aggregatedStream.toStream()
				.peek((windowed, value) -> println(windowed.key() + " - " + value))
				.map((keyWindowed, value) -> KeyValue.pair(keyWindowed.key(), value))
				.to(OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.Long()));
	}
}
