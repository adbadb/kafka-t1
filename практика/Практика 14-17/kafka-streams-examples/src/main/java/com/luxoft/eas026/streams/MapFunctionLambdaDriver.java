package com.luxoft.eas026.streams;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class MapFunctionLambdaDriver {

	public static void main(final String[] args) throws IOException {
		final String[] industries = { "engineering", "telco", "finance", "health", "science", "manufacturing",
				"education", "retail" };

		final Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	
		try (final KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

			for (final String industry : industries) {
				producer.send(new ProducerRecord<>("TextLinesTopic", null, industry));
				producer.flush();
			}
		}
	}

}
