package pl.pb.kafkaconnectexample.postgress;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.record.RecordBatch;

import java.time.Duration;

import static pl.pb.kafkaconnectexample.config.Commons.println;
import static pl.pb.kafkaconnectexample.config.KafkaConfig.POSTGRES_SOURCED_USER;
import static pl.pb.kafkaconnectexample.config.KafkaConfig.getConsumer;

public class KafkaConsumerExample {

	public static void main(final String[] args) {

		// create consumer
		final KafkaConsumer<String, RecordBatch> consumer = getConsumer(POSTGRES_SOURCED_USER);

		// consume data
		while (true) {
			consumer.poll(Duration.ofMillis(100))
					.forEach(consumerRecord -> println("Topic: %s,\n Key: %s,\n Value: %s,\n Partition: %s,\n Offset: %s\n",
							consumerRecord.topic(),
							consumerRecord.key(),
							consumerRecord.value(),
							consumerRecord.partition(),
							consumerRecord.offset()
					));
		}
	}
}
