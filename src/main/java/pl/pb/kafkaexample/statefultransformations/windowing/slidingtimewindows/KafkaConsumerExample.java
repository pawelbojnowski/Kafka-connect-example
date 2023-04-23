package pl.pb.kafkaexample.statefultransformations.windowing.slidingtimewindows;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;

import static pl.pb.kafkaexample.config.Commons.println;

public class KafkaConsumerExample {

	public static void main(final String[] args) {

		// create consumer
		final KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerConfig());

		// add subscribed topic(s)
		consumer.subscribe(List.of(KafkaConfig.OUTPUT_TOPIC_1, KafkaConfig.OUTPUT_TOPIC_2));

		// consume data
		while (true) {
			consumer.poll(Duration.ofMillis(100))
					.forEach(consumerRecord -> println("Topic: %s, Key: %s, Value: %-10s Partition: %s, Offset: %s",
							consumerRecord.topic(),
							consumerRecord.key(),
							consumerRecord.value(),
							consumerRecord.partition(),
							consumerRecord.offset()
					));
		}
	}

}
