package pl.pb.kafkaexample.globalktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.globalktable.KafkaConfig.*;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {

		//Define store
		final Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(
						"example-global-store" /* table/store name */)
				.withKeySerde(Serdes.String()) /* key serde */
				.withValueSerde(Serdes.String());/* value serde */

		//Global KTable
		final GlobalKTable<String, String> right = builder.globalTable(INPUT_TOPIC_1, materialized /* value serde */);

		//KStream
		final KStream<String, String> left = builder.stream(INPUT_TOPIC_2);

		//Join KStream with GlobalKTable
		final KTable<String, String> joined = left.join(right,
						(leftKey, leftValue) -> leftKey,
						(leftValue, rightValue) -> rightValue + " " + leftValue /* ValueJoiner */
				)
				.peek((key, value) -> println("Processed message: " + key + " = " + value))
				.toTable();

		//Forward result
		joined.toStream().to(OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.String()));
	}

}
