package pl.pb.kafkaconnectexample.proto;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import pl.pb.kafkamodel.user.User;

import java.util.concurrent.CountDownLatch;

import static pl.pb.kafkaconnectexample.proto.KafkaProtoConfigProperties.*;

public class KafkaConfig {

	public static final String POSTGRES_SOURCED_USER = "postgres.connector.source.user";
	public static final String POSTGRES_SINK_CLIENT = "postgres.connector.sink.client";

	private KafkaConfig() {
	}

	public static void runStreams(Topology build) {
		KafkaStreams kafkaStreams = new KafkaStreams(build, getStreamsConfig());
		closeKafkaStreams(kafkaStreams);
	}

	public static KafkaConsumer getConsumer() {
		return new KafkaConsumer<String, User>(getConsumerConfig());
	}

	public static KafkaProducer getProducer() {
		return new KafkaProducer<String, Object>(getProducerConfig());
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
