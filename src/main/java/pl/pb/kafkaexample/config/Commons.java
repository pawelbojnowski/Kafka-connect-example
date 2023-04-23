package pl.pb.kafkaexample.config;

import org.apache.kafka.streams.KafkaStreams;

import java.util.concurrent.CountDownLatch;

public class Commons {

	private Commons() {
	}

	public static void print(final String format, final Object... args) {
		System.out.print(String.format(format, args));
	}

	public static void println(final String format, final Object... args) {
		System.out.println(String.format(format, args));
	}

	public static void println() {
		System.out.println();
	}

	public static void sleep(final int time) {
		try {
			Thread.sleep(time);
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	public static void closeKafkaStreams(final KafkaStreams streams) {
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
