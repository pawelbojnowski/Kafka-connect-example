package pl.pb.kafkaexample.test;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class CustomMaxAggregatorProcessor implements Processor<String, Long, String, Long> {

	ProcessorContext context;
	private KeyValueStore<String, Long> store;

	@SuppressWarnings("unchecked")
	@Override
	public void init(final ProcessorContext context) {
		this.context = context;
		context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, time -> flushStore(time));
		context.schedule(Duration.ofSeconds(10), PunctuationType.STREAM_TIME, time -> flushStore(time));
		store = (KeyValueStore<String, Long>) context.getStateStore("aggStore");
	}


	@Override
	public void process(final Record<String, Long> record) {
		final Long oldValue = store.get(record.key());
		if (oldValue == null || record.value() > oldValue) {
			store.put(record.key(), record.value());
		}
	}

	private void flushStore(final long time) {
		final KeyValueIterator<String, Long> it = store.all();
		while (it.hasNext()) {
			final KeyValue<String, Long> next = it.next();
			context.forward(new Record(next.key, next.value, time));
		}
	}

	@Override
	public void close() {
	}
}
