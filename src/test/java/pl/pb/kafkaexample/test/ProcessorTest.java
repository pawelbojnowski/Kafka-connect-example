package pl.pb.kafkaexample.test;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * example from https://kafka.apache.org/documentation/streams/developer-guide/testing.html
 */
public class ProcessorTest {

	private WordCountProcessor processorUnderTest;
	private MockProcessorContext context;
	private KeyValueStore<String, Integer> store;
	private StateStoreContext stateStoreContext;

	@BeforeEach
	public void setup() {

		processorUnderTest = new WordCountProcessor();
		context = new MockProcessorContext();

		store = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(WordCountProcessor.STATE_STORE), Serdes.String(), Serdes.Integer())
				.withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
				.build();

		stateStoreContext = context.getStateStoreContext();
		store.init(stateStoreContext, store);
		processorUnderTest.init(context);
	}

	@AfterEach
	public void reset() {
		context.resetForwards();
		context.resetCommit();
	}

	@Test
	public void shouldReturnCountOfWord() {
		//when
		final Record<String, String> record = new Record("textKey", "Text text", 0);
		processorUnderTest.process(record);

		//then
		//captures forwarded message
		final Iterator<CapturedForward> forwarded = context.forwarded().iterator();
		assertEquals(forwarded.next().record(), new Record("text", 1, 0));
		assertEquals(forwarded.next().record(), new Record("text", 2, 0));
		assertFalse(forwarded.hasNext());

		//captures count forwarded message
		assertEquals(context.forwarded().size(), 2);

		//captures store
		assertEquals(store.get("text"), 2);

		//captures commit
		assertTrue(context.committed());
	}

}
