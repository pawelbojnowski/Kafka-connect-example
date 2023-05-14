package pl.pb.kafkaconnectexample.cassandra.proto;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.concurrent.CountDownLatch;

import static pl.pb.kafkaconnectexample.cassandra.proto.KafkaConfigProperties.*;

public class KafkaConfig {

	public static final String CASSANDRA_SOURCED_USER = "cassandra.connector.source.user";
	public static final String CASSANDRA_SINK_USER_INSERT = "cassandra.connector.sink.user.insert";
	public static final String CASSANDRA_SINK_USER_UPDATE = "cassandra.connector.sink.user.update";
	public static final String CASSANDRA_CONNECTOR_SOURCE_USER_QUERY_TIMESTAMP = "cassandra.connector.source.user.query.timestamp";
	public static final String CASSANDRA_CONNECTOR_SOURCE_USER_QUERY_INCREMENTING = "cassandra.connector.source.user.query.incrementing";
	public static final String CASSANDRA_SINK_CLIENT = "cassandra.connector.sink.client";

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
