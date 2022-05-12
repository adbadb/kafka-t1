package com.luxoft.eas026.streams;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class PurchaseStatisticsDriver {

	public static String[] products = { "kettle", "hair dryer", "toaster", "grill" };

	public static void main(final String[] args) throws IOException, InterruptedException {

		final Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

		try (final KafkaProducer<String, Purchase> producer = new KafkaProducer<>(props)) {

			long id = 0;
			for (int j = 0; j < 10; j++) {
				final String product = products[new Random().nextInt(products.length)];
				final int customer = new Random().nextInt(3);
				Purchase purchase = Purchase.newBuilder().setId(++id).setProduct(product).setAmount(1).setSum(10)
						.setCustomerId(customer).build();
				producer.send(new ProducerRecord<>("Purchases", String.valueOf(id), purchase));

			}

		} 
	}

}
