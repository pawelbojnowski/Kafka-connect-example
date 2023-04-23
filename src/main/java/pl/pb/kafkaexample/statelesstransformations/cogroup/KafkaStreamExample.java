package pl.pb.kafkaexample.statelesstransformations.cogroup;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statelesstransformations.cogroup.KafkaConfig.*;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {

		try {
			final Initializer<String> initializer = () -> "";

			final Aggregator<String, String, String> aggregator = (key, newValue, count) -> {
				println("Aggregation by key: " + key + ", newValue: " + newValue + ", count: " + count);
				return count + "," + newValue;
			};

			final KStream<String, String> cart = builder.stream(INPUT_TOPIC_1);
			final KGroupedStream<String, String> groupedCart = cart.groupByKey();
			final KTable<String, String> customers = groupedCart.cogroup(aggregator)
					.aggregate(initializer);

			customers.toStream().to(OUTPUT_TOPIC_1);
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}
}
