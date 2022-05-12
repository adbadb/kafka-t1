package com.luxoft.eas026.module4;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class AvroConsumer {

	private final static String BOOTSTRAP_SERVERS = ":9092";
	private final static String GROUP_ID = "avro-test";
	private final static String OFFSET_RESET = "earliest";
	private final static String TOPIC = "company"; 

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
		props.put("schema.registry.url", "http://localhost:8081");

		final KafkaConsumer<String, Company> consumer = new KafkaConsumer<>(props);

		try {
			consumer.subscribe(Collections.singleton(TOPIC));
			while (true) {
				ConsumerRecords<String, Company> records = consumer.poll(Duration.ofSeconds(2));
				for (ConsumerRecord<String, Company> data : records) {
					System.out.printf("key=%s, value=%s => partition=%d, offset=%d\n", data.key(), data.value(),
							data.partition(), data.offset());
				}
			}
		} catch (Exception e) {
			System.out.printf("Exception %s\n", e.getMessage());
		} finally {
			consumer.close();
		}
	}
}
