package pl.pb.kafkaexample.statelesstransformations.cogroup;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;

import static pl.pb.kafkaexample.config.Commons.println;

public class KafkaConsumerExample {

	public static void main(final String[] args) {

		// create consumer
		final KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerConfig());

		// add subscribed topic(s)
		consumer.subscribe(List.of(KafkaConfig.OUTPUT_TOPIC_1));

		// consume data
		while (true) {
			final ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(100));

			for (final ConsumerRecord<String, Integer> consumerRecord : records) {
				println("-----------------------------------------------------");
				println("Data: ");
				println("Key  : " + consumerRecord.key());
				println("Value: " + consumerRecord.value());
				println("Partition: " + consumerRecord.partition() + ", Offset:" + consumerRecord.offset());
			}
		}
	}

}
