package pl.pb.kafkaexample.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import static pl.pb.kafkaexample.config.Commons.closeKafkaStreams;
import static pl.pb.kafkaexample.processor.KafkaConfig.*;

public class KafkaStreamExample {

	public static final String SOURCE = "Source";
	public static final String PROCESS = "Process";
	public static final String SINK = "Sink";

	public static void main(final String[] args) {

		final Topology topology = createTopology();

		final KafkaStreams streams = new KafkaStreams(topology, getStreamsConfig());

		closeKafkaStreams(streams);
	}

	static Topology createTopology() {

		final StoreBuilder<KeyValueStore<String, Integer>> countStoreBuilder = Stores.keyValueStoreBuilder(
				Stores.inMemoryKeyValueStore(WordCountProcessor.STATE_STORE),
				Serdes.String(),
				Serdes.Integer()
		);

		return new Topology()
				.addSource(SOURCE, INPUT_TOPIC_1)
				.addProcessor(PROCESS, () -> new WordCountProcessor(), SOURCE)
				.addStateStore(countStoreBuilder, PROCESS)
				.addSink(SINK, OUTPUT_TOPIC_1, PROCESS);

	}

}
