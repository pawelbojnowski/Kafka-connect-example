package pl.pb.kafkaexample.statefultransformations.joining.kstream_kstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.time.Duration;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statefultransformations.joining.kstream_kstream.KafkaConfig.*;

public class KafkaStreamExample {

	private static final ValueJoiner<String, String, String> STRING_STRING_STRING_VALUE_JOINER = (leftValue, rightValue) -> "[" + leftValue + ", " + rightValue + "]";
	private static final JoinWindows JOIN_WINDOWS = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(3));

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {

		final KStream<String, String> left = builder.stream(INPUT_TOPIC_LEFT);  //key: "ADMIN", value: "xyz@email.com
		final KStream<String, String> right = builder.stream(INPUT_TOPIC_RIGHT); //key: "ADMIN", value: "Full access"

		join(left, right);
		leftJoin(left, right);
		outerJoin(left, right);
	}

	private static void join(final KStream<String, String> left, final KStream<String, String> right) {
		final KStream<String, String> joined = left.join(right, // <--- inner join
				STRING_STRING_STRING_VALUE_JOINER, /* ValueJoiner */
				JOIN_WINDOWS);

		forward(joined, OUTPUT_TOPIC_1);
	}

	private static void leftJoin(final KStream<String, String> left, final KStream<String, String> right) {
		final KStream<String, String> joined = left.leftJoin(right, // <--- left join
				STRING_STRING_STRING_VALUE_JOINER, /* ValueJoiner */
				JOIN_WINDOWS);

		forward(joined, OUTPUT_TOPIC_2);
	}

	private static void outerJoin(final KStream<String, String> left, final KStream<String, String> right) {
		final KStream<String, String> joined = left.outerJoin(right, // <--- left join
				STRING_STRING_STRING_VALUE_JOINER, /* ValueJoiner */
				JOIN_WINDOWS);

		forward(joined, OUTPUT_TOPIC_3);
	}

	private static void forward(final KStream<String, String> joined, final String outputTopic1) {
		joined.peek((key, value) -> println(key + ": " + value))
				.to(outputTopic1, Produced.with(Serdes.String(), Serdes.String()));
	}
}
