package pl.pb.kafkaexample.emitrate;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.Suppressed.EagerBufferConfig;

import java.time.Duration;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.emitrate.KafkaConfig.INPUT_TOPIC_1;
import static pl.pb.kafkaexample.emitrate.KafkaConfig.getStreamsConfig;

public class KafkaStreamExample {

	public static void main(final String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		createStream(builder);

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static void createStream(final StreamsBuilder builder) {
		final KStream<String, String> source = builder.stream(INPUT_TOPIC_1);

		final Duration timeToWaitForMoreEvents = Duration.ofSeconds(10);
		final EagerBufferConfig eagerBufferConfig = BufferConfig.maxBytes(10_000L).emitEarlyWhenFull();
		final Suppressed<String> suppressed = Suppressed.untilTimeLimit(timeToWaitForMoreEvents, eagerBufferConfig);

		source
				.groupByKey()
				.count()
				.suppress(suppressed) // <- suppression
				.toStream()
				.peek((key, value) -> println(key + " = " + value))
				.to(KafkaConfig.OUTPUT_TOPIC_1);
	}

}
