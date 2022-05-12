package com.luxoft.eas026.module4;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class AvroProducer {

	private final static String BOOTSTRAP_SERVERS = ":9092";

	private final static String TOPIC = "company";

	private final static String CLIENT_ID = "avro-test";

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		props.put("schema.registry.url","http://localhost:8081");

		Company comp = new Company();
		comp.setTradeNumber(12345);
		comp.setRegisteredName("MyCompany");
		final Producer<String, Company> producer = new KafkaProducer<>(props);
		try {
			final ProducerRecord<String, Company> data = new ProducerRecord<>(TOPIC, "12345", comp);
			try {
				RecordMetadata meta = producer.send(data).get();
				System.out.printf("key=%s, value=%s => partition=%d, offset=%d\n", data.key(), data.value(),
						meta.partition(), meta.offset());
			} catch (InterruptedException | ExecutionException e) {
				System.out.printf("Exception %s\n", e.getMessage());
			}
		} finally {
			producer.flush();
			producer.close();
		}
	}
}
