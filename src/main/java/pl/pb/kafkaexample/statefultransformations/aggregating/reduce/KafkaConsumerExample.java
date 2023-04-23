package pl.pb.kafkaexample.statefultransformations.aggregating.reduce;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;

import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statefultransformations.aggregating.reduce.KafkaConfig.*;

public class KafkaConsumerExample {

	public static void main(final String[] args) {

		// create consumer
		final KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(getConsumerConfig());

		// add subscribed topic(s)
		consumer.subscribe(List.of(OUTPUT_TOPIC_1, OUTPUT_TOPIC_2));

		// consume data
		while (true) {
			final ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(100));

			for (final ConsumerRecord<String, Long> consumerRecord : records) {
				println("-----------------------------------------------------");
				println("Topic: " + consumerRecord.topic());
				println("Data: ");
				println("Key  : " + consumerRecord.key());
				println("Value: " + consumerRecord.value());
				println("Partition: " + consumerRecord.partition() + ", Offset:" + consumerRecord.offset());
			}
		}
	}

}
