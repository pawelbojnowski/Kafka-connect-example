package pl.pb.kafkaconnectexample.test;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class CustomMaxAggregatorProcessorSupplier implements ProcessorSupplier<String, Long, String, Long> {

	@Override
	public Processor<String, Long, String, Long> get() {
		return new CustomMaxAggregatorProcessor();
	}
}
