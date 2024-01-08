package pl.pb.kafkaconnectexample.cassandra.avro;

import org.apache.kafka.clients.producer.KafkaProducer;

import static pl.pb.kafkaconnectexample.cassandra.avro.KafkaConfigProperties.getProducerConfig;

public class KafkaConfig {

	public static final String CASSANDRA_SINK_CLIENT = "cassandra_connector_sink_user";

	private KafkaConfig() {
	}

	public static <K, V> KafkaProducer getProducer() {
		return new KafkaProducer<K, V>(getProducerConfig());
	}

}
