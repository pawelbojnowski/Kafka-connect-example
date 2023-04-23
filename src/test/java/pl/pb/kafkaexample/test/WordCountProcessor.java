package pl.pb.kafkaexample.test;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import pl.pb.kafkaexample.config.Commons;

public class WordCountProcessor implements Processor<String, String, String, String> {

	public static final String STATE_STORE = "Counts";
	private KeyValueStore<String, Integer> kvStore;
	private ProcessorContext<String, String> context;

	@Override
	public void init(final ProcessorContext<String, String> context) {
		this.context = context;
		kvStore = context.getStateStore(STATE_STORE);
	}

	@Override
	public void process(final Record<String, String> record) {
		Commons.println("Process: " + record.value());
		final String[] words = record.value().toLowerCase().split("\\W+");

		for (final String word : words) {
			final Integer oldValue = kvStore.get(word);

			if (oldValue == null) {
				kvStore.put(word, 1);
			} else {
				kvStore.put(word, oldValue + 1);
			}

			context.forward(new Record(word, kvStore.get(word), 0));
		}
		context.commit();
	}

	@Override
	public void close() {
		// close any resources managed by this processor
		// Note: Do not close any StateStores as these are managed by the library
	}
}
