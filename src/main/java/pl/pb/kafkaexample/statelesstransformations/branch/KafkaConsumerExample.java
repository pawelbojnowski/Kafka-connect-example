package pl.pb.kafkaexample.statelesstransformations.branch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;

import static pl.pb.kafkaexample.config.Commons.println;
import static pl.pb.kafkaexample.statelesstransformations.branch.KafkaConfig.*;

public class KafkaConsumerExample {

	public static void main(final String[] args) {

		// create consumer
		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerConfig());

		// add subscribed topic(s)
		consumer.subscribe(List.of(OUTPUT_TOPIC_1, OUTPUT_TOPIC_2, OUTPUT_TOPIC_3));

		// consume data
		while (true) {
			final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

			for (final ConsumerRecord<String, String> consumerRecord : consumerRecords) {
				println("-----------------------------------------------------");
				println("Data: ");
				println("Key  : " + consumerRecord.key());
				println("Value: " + consumerRecord.value());
				println("Partition: " + consumerRecord.partition() + ", Offset:" + consumerRecord.offset());
			}
		}
	}

}
