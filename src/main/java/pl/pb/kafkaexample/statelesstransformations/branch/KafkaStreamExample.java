package pl.pb.kafkaexample.statelesstransformations.branch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statelesstransformations.branch.KafkaConfig.INPUT_TOPIC_1;
import static pl.pb.kafkaexample.statelesstransformations.branch.KafkaConfig.getStreamsConfig;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {
		final KStream<String, String> source = builder.stream(INPUT_TOPIC_1);

		final Map<String, KStream<String, String>> branchedKStreamByUsersType = source.split(Named.as("BRANCH_"))
				.branch((s, s2) -> s.equals("REGULAR_USER"), Branched.as("REGULAR_USER"))
				.branch((s, s2) -> s.equals("AGENT_USER"), Branched.as("AGENT_USER"))
				.branch((s, s2) -> s.equals("ADMIN_USER"), Branched.as("ADMIN_USER"))
				.defaultBranch();

		branchedKStreamByUsersType.get("BRANCH_REGULAR_USER")
				.map((key, value) -> new KeyValue<>(key, key + " = " + value + " - which has minimum Access ;) "))
				.to(KafkaConfig.OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.String()));

		branchedKStreamByUsersType.get("BRANCH_AGENT_USER")
				.map((key, value) -> new KeyValue<>(key, key + " = " + value + " - which can do a little bit more tha regular user ;) "))
				.to(KafkaConfig.OUTPUT_TOPIC_3, Produced.with(Serdes.String(), Serdes.String()));

		branchedKStreamByUsersType.get("BRANCH_ADMIN_USER")
				.map((key, value) -> new KeyValue<>(key, key + " = " + value + " - which is able do everything ;) "))
				.to(KafkaConfig.OUTPUT_TOPIC_2, Produced.with(Serdes.String(), Serdes.String()));

		branchedKStreamByUsersType.get("BRANCH_0")
				.foreach((s, s2) -> println("Unrecognizable user: " + s2 + " - which will not be process"));

	}
}
