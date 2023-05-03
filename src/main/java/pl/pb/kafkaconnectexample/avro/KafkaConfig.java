package pl.pb.kafkaconnectexample.avro;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.concurrent.CountDownLatch;

import static pl.pb.kafkaconnectexample.avro.KafkaConfigProperties.*;

public class KafkaConfig {

	public static final String POSTGRES_SOURCED_USER = "postgres.connector.source.user";
	public static final String POSTGRES_SINK_USER_INSERT = "postgres.connector.sink.user.insert";
	public static final String POSTGRES_SINK_USER_UPDATE = "postgres.connector.sink.user.update";
	public static final String POSTGRES_CONNECTOR_SOURCE_USER_QUERY_TIMESTAMP = "postgres.connector.source.user.query.timestamp";
	public static final String POSTGRES_CONNECTOR_SOURCE_USER_QUERY_INCREMENTING = "postgres.connector.source.user.query.incrementing";
	public static final String POSTGRES_SINK_CLIENT = "postgres.connector.sink.client";

	private KafkaConfig() {
	}

	public static void runStreams(Topology build) {
		KafkaStreams kafkaStreams = new KafkaStreams(build, getStreamsConfig());
		closeKafkaStreams(kafkaStreams);
	}

	public static <K, V> KafkaConsumer getConsumer() {
		return new KafkaConsumer<K, V>(getConsumerConfig());
	}

	public static <K, V> KafkaProducer getProducer() {
		return new KafkaProducer<K, V>(getProducerConfig());
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
