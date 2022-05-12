package com.luxoft.eas026.streams;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class PurchaseStatisticsExample {

	public static final String SCHEMA_REGISTRY_URL="http://localhost:8081";
	
	public static void main(final String[] args) throws Exception {
		final Properties streamsConfiguration = new Properties();

		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "count-purchases-example");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "count-purchases-example-client");

		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
		streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/streams/");
		streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

		final StreamsBuilder builder = new StreamsBuilder();

		final KStream<String, Purchase> purchases = builder.stream("Purchases");

		final Serde<Purchase> specificAvroSerde = new SpecificAvroSerde<>();
		final boolean isKeySerde = false;
		specificAvroSerde.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
				SCHEMA_REGISTRY_URL), isKeySerde);

		purchases.groupBy((key, value) -> value.getProduct().toString(),
				Grouped.with(Serdes.String(), specificAvroSerde)).count().mapValues(v->v.toString()).toStream()
				.to("PurchaseStatistics", Produced.with(Serdes.String(), Serdes.String()));

		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.cleanUp();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

}
