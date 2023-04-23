package pl.pb.kafkaexample.statefultransformations.joining.kstream_globalktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statefultransformations.joining.kstream_globalktable.KafkaConfig.*;

public class KafkaStreamExample {

	private static final ValueJoiner<String, String, String> VALUE_JOINER = (leftValue, rightValue) -> "[" + leftValue + ", " + rightValue + "]";
	private static final KeyValueMapper<String, String, String> KEY_VALUE_MAPPER = (leftKey, leftValue) -> leftKey;

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {

		final KStream<String, String> left = builder.stream(INPUT_TOPIC_LEFT);
		final GlobalKTable<String, String> right = builder.globalTable(INPUT_TOPIC_RIGHT);

		innerJoin(left, right);
		/**
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [A, null]  Partition: 0, Offset: 0
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [B, a]     Partition: 0, Offset: 1
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [B, a]     Partition: 0, Offset: 2
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [C, null]  Partition: 0, Offset: 3
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [D, d]     Partition: 0, Offset: 4
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [D, d]     Partition: 0, Offset: 5
		 */

		leftJoin(left, right);
		/** Result
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [A, null]  Partition: 0, Offset: 0
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [B, a]     Partition: 0, Offset: 1
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [B, a]     Partition: 0, Offset: 2
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [C, null]  Partition: 0, Offset: 3
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [D, d]     Partition: 0, Offset: 4
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [D, d]     Partition: 0, Offset: 5
		 */
	}

	private static void innerJoin(final KStream<String, String> left, final GlobalKTable<String, String> right) {

		final KStream<String, String> joined = left.join(right,
				KEY_VALUE_MAPPER, /* derive a (potentially) new key by which to lookup against the table */
				VALUE_JOINER /* ValueJoiner */
		);

		forward(joined, OUTPUT_TOPIC_1);
	}

	private static void leftJoin(final KStream<String, String> left, final GlobalKTable<String, String> right) {
		final KStream<String, String> joined = left.leftJoin(right,
				KEY_VALUE_MAPPER, /* derive a (potentially) new key by which to lookup against the table */
				VALUE_JOINER /* ValueJoiner */
		);

		forward(joined, OUTPUT_TOPIC_1);
	}

	private static void forward(final KStream<String, String> joined, final String outputTopic2) {
		joined
				.peek((key, value) -> println(key + ": " + value))
				.to(outputTopic2, Produced.with(Serdes.String(), Serdes.String()));
	}
}
