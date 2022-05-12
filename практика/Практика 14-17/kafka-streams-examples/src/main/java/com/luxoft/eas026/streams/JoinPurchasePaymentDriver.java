package com.luxoft.eas026.streams;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class JoinPurchasePaymentDriver {

	public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

	public static void main(final String[] args) throws IOException {
		final Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

		try (final KafkaProducer<String, Payment> producer = new KafkaProducer<>(props)) {
			int id = 0;
			for (int j = 0; j < 10; j += 2) {
				Payment payment = Payment.newBuilder().setId(j + 100).setPurchaseId(j).build();
				producer.send(new ProducerRecord<>("Payments", String.valueOf(id), payment));
			}
		}
	}

}
