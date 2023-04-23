package pl.pb.kafkaexample.statefultransformations.joining.ktable_ktable_foreign_key;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.function.Function;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statefultransformations.joining.ktable_ktable_foreign_key.KafkaConfig.*;

public class KafkaStreamExample {

	private static final Function<String, String> FOREIGN_KEY_EXTRACTOR = foreignKey -> foreignKey;
	private static final ValueJoiner<String, String, String> VALUE_JOINER = (leftValue, rightValue) -> "[" + leftValue + ", " + rightValue + "]";

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
	}

	private static void join(final KTable<String, String> left, final KTable<String, String> right) {

		final KTable<String, String> joined = left.join(right,// <--- inner join
				FOREIGN_KEY_EXTRACTOR,
				VALUE_JOINER
		);

		forward(joined, OUTPUT_TOPIC_1);
	}

	private static void leftJoin(final KTable<String, String> left, final KTable<String, String> right) {
		final KTable<String, String> joined = left.leftJoin(right, // <--- left join
				FOREIGN_KEY_EXTRACTOR,
				VALUE_JOINER
		);

		forward(joined, OUTPUT_TOPIC_2);
	}

	private static void forward(final KTable<String, String> joined, final String outputTopic2) {
		joined.toStream()
				.peek((key, value) -> println(key + ": " + value))
				.to(outputTopic2, Produced.with(Serdes.String(), Serdes.String()));
	}
}
