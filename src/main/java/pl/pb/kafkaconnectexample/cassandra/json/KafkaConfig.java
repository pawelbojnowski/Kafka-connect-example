package pl.pb.kafkaconnectexample.cassandra.json;

import org.apache.kafka.clients.producer.KafkaProducer;

import static pl.pb.kafkaconnectexample.cassandra.json.KafkaConfigProperties.*;

public class KafkaConfig {

	public static final String CASSANDRA_SINK_CLIENT = "cassandra_connector_sink_user";

	private KafkaConfig() {
	}

	public static KafkaProducer getProducer() {
		return new KafkaProducer(getProducerConfig());
	}
}
