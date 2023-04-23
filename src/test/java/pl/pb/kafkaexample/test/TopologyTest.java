package pl.pb.kafkaexample.test;


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * example from https://kafka.apache.org/documentation/streams/developer-guide/testing.html
 */
public class TopologyTest {

	private TopologyTestDriver testDriver;
	private TestInputTopic<String, Long> inputTopic;
	private TestOutputTopic<String, Long> outputTopic;
	private KeyValueStore<String, Long> store;

	private final Serde<String> stringSerde = new Serdes.StringSerde();
	private final Serde<Long> longSerde = new Serdes.LongSerde();

	@BeforeEach
	public void setup() {
		final Topology topology = new Topology();
		topology.addSource("sourceProcessor", "input-topic");
		topology.addProcessor("aggregator", new CustomMaxAggregatorProcessorSupplier(), "sourceProcessor");
		topology.addStateStore(
				Stores.keyValueStoreBuilder(
						Stores.inMemoryKeyValueStore("aggStore"),
						Serdes.String(),
						Serdes.Long()).withLoggingDisabled(), // need to disable logging to allow store pre-populating
				"aggregator");
		topology.addSink("sinkProcessor", "result-topic", "aggregator");

		// setup test driver
		final Properties props = new Properties();
		props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		testDriver = new TopologyTestDriver(topology, props);

		// setup test topics
		inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), longSerde.serializer());
		outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), longSerde.deserializer());

		// pre-populate store
		store = testDriver.getKeyValueStore("aggStore");
		store.put("a", 21L);
	}

	@AfterEach
	public void tearDown() {
		testDriver.close();
	}

	@Test
	public void shouldNotUpdateStoreForSmallerValue() {
		inputTopic.pipeInput("a", 1L);
		assertEquals(store.get("a"), 21L);
		assertEquals(outputTopic.readKeyValue(), new KeyValue<>("a", 21L));
		assertTrue(outputTopic.isEmpty());
	}

	@Test
	public void shouldNotUpdateStoreForLargerValue() {
		inputTopic.pipeInput("a", 42L);
		assertEquals(store.get("a"), 42L);
		assertEquals(outputTopic.readKeyValue(), new KeyValue<>("a", 42L));
		assertTrue(outputTopic.isEmpty());
	}

	@Test
	public void shouldUpdateStoreForNewKey() {
		inputTopic.pipeInput("b", 21L);
		assertEquals(store.get("b"), 21L);
		assertEquals(outputTopic.readKeyValue(), new KeyValue<>("a", 21L));
		assertEquals(outputTopic.readKeyValue(), new KeyValue<>("b", 21L));
		assertTrue(outputTopic.isEmpty());
	}

	@Test
	public void shouldPunctuateIfEvenTimeAdvances() {
		final Instant recordTime = Instant.now();
		inputTopic.pipeInput("a", 1L, recordTime);
		assertEquals(outputTopic.readKeyValue(), new KeyValue<>("a", 21L));

		inputTopic.pipeInput("a", 1L, recordTime);
		assertTrue(outputTopic.isEmpty());

		inputTopic.pipeInput("a", 1L, recordTime.plusSeconds(10L));
		assertEquals(outputTopic.readKeyValue(), new KeyValue<>("a", 21L));
		assertTrue(outputTopic.isEmpty());
	}

}
