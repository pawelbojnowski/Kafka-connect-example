package pl.pb.kafkaexample.statefultransformations.joining.kstream_ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statefultransformations.joining.kstream_ktable.KafkaConfig.*;

public class KafkaStreamExample {

	private static final ValueJoiner<String, String, String> VALUE_JOINER = (leftValue, rightValue) -> "[" + leftValue + ", " + rightValue + "]";

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {

		final KStream<String, String> left = builder.stream(INPUT_TOPIC_LEFT);
		final KTable<String, String> right = builder.table(INPUT_TOPIC_RIGHT);

		innerJoin(left, right);
		/**
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [B, a]     Partition: 0, Offset: 0
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [D, d]     Partition: 0, Offset: 1
		 */

		leftJoin(left, right);
		/** Result
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [A, null]  Partition: 0, Offset: 0
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [B, a]     Partition: 0, Offset: 1
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [C, null]  Partition: 0, Offset: 2
		 * Topic: kafka_example_output_1, Key: joiningKafka, Value: [D, d]     Partition: 0, Offset: 3
		 */
	}

	private static void innerJoin(final KStream<String, String> left, final KTable<String, String> right) {

		final KStream<String, String> joined = left.join(right,
				VALUE_JOINER /* ValueJoiner */
		);

		forward(joined, OUTPUT_TOPIC_1);
	}

	private static void leftJoin(final KStream<String, String> left, final KTable<String, String> right) {
		final KStream<String, String> joined = left.leftJoin(right,
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
