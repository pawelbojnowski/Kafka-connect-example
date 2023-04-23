package pl.pb.kafkaexample.statefultransformations.aggregating.count;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

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

		kGroupedStream(source);
		kGroupedTable(source);
	}

	private static void kGroupedStream(final KStream<String, String> source) {
		final KGroupedStream<String, String> groupedStream = source.groupByKey();

		final KTable<String, Long> aggregatedStream = groupedStream.count();

		aggregatedStream.toStream()
				.peek((key, value) -> println(key + " - " + value))
				.to(KafkaConfig.OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.Long()));
	}

	private static void kGroupedTable(final KStream<String, String> source) {
		final KGroupedTable<String, String> groupedTable = source.toTable().groupBy(KeyValue::pair);

		final KTable<String, Long> aggregatedStream = groupedTable.count();

		aggregatedStream.toStream()
				.peek((key, value) -> println(key + " - " + value))
				.to(KafkaConfig.OUTPUT_TOPIC_2, Produced.with(Serdes.String(), Serdes.Long()));
	}
}
