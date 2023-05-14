package pl.pb.kafkaconnectexample.cassandra.jsonschema;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.concurrent.CountDownLatch;

import static pl.pb.kafkaconnectexample.cassandra.jsonschema.KafkaConfigProperties.*;

public class KafkaConfig {

	public static final String CASSANDRA_SOURCED_USER = "user";
	public static final String CASSANDRA_SINK_CLIENT = "user";

	private KafkaConfig() {
	}

	public static void runStreams(Topology build) {
		KafkaStreams kafkaStreams = new KafkaStreams(build, getStreamsConfig());
		closeKafkaStreams(kafkaStreams);
	}

	public static KafkaConsumer getConsumer() {
		return new KafkaConsumer(getConsumerConfig());
	}

	public static KafkaProducer getProducer() {
		return new KafkaProducer(getProducerConfig());
	}


	private static void closeKafkaStreams(final KafkaStreams streams) {
		final CountDownLatch latch = new CountDownLatch(1);
		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("kafka-stream-example-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (final Exception e) {
			System.exit(1);
		}
		System.exit(0);
	}
}
