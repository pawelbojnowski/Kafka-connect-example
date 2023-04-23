package pl.pb.kafkaexample.statefultransformations.joining.ktable_ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statefultransformations.joining.ktable_ktable.KafkaConfig.*;

public class KafkaStreamExample {

	public static final ValueJoiner<String, String, String> STRING_STRING_STRING_VALUE_JOINER = (leftValue, rightValue) -> "[" + leftValue + ", " + rightValue + "]";

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {

		final KTable<String, String> left = builder.table(INPUT_TOPIC_LEFT);
		final KTable<String, String> right = builder.table(INPUT_TOPIC_RIGHT);

		join(left, right);
		leftJoin(left, right);
		outerJoin(left, right);
	}

	private static void join(final KTable<String, String> left, final KTable<String, String> right) {
		final KTable<String, String> joined = left.join(right, // <--- inner join
				STRING_STRING_STRING_VALUE_JOINER /* ValueJoiner */
		);

		forward(joined, OUTPUT_TOPIC_1);
	}

	private static void leftJoin(final KTable<String, String> left, final KTable<String, String> right) {
		final KTable<String, String> joined = left.leftJoin(right, // <--- left join
				STRING_STRING_STRING_VALUE_JOINER /* ValueJoiner */
		);

		forward(joined, OUTPUT_TOPIC_2);
	}

	private static void outerJoin(final KTable<String, String> left, final KTable<String, String> right) {
		final KTable<String, String> joined = left.outerJoin(right, // <--- left join
				STRING_STRING_STRING_VALUE_JOINER /* ValueJoiner */
		);
		forward(joined, OUTPUT_TOPIC_3);
	}

	private static void forward(final KTable<String, String> joined, final String outputTopic1) {
		joined.toStream()
				.peek((key, value) -> println(key + ": " + value))
				.to(outputTopic1, Produced.with(Serdes.String(), Serdes.String()));
	}
}
