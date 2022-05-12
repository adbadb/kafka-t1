package com.luxoft.eas026.streams;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class JoinPurchasePayments {

	public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

	public static void main(final String[] args) throws Exception {
		final Properties streamsConfiguration = new Properties();

		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-purchases-example");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "join-purchases-example-client");

		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
		streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/streams/");
		streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

		final StreamsBuilder builder = new StreamsBuilder();

		final KStream<String, Purchase> purchases = builder.stream("Purchases");
		final KStream<String, Payment> payments = builder.stream("Payments");

		final boolean isKeySerde = false;
		Map<String, String> map = Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
				SCHEMA_REGISTRY_URL);

		final Serde<Purchase> purchaseAvroSerde = new SpecificAvroSerde<>();
		purchaseAvroSerde.configure(map, isKeySerde);

		final Serde<Payment> paymentAvroSerde = new SpecificAvroSerde<>();
		paymentAvroSerde.configure(map, isKeySerde);

		final KStream<String, PayedPurchase> joined = purchases.join(payments.selectKey((k, v) -> String.valueOf(v.getPurchaseId())),
				(purchase, payment) -> PayedPurchase
						.newBuilder().setPaymentId(payment.getId()).setPurchaseId(payment.getPurchaseId())
						.setProduct(purchase.getProduct()).build(), /* ValueJoiner */
				JoinWindows.of(Duration.ofDays(1)), StreamJoined.with(Serdes.String(), /* key */
						purchaseAvroSerde, /* left value */
						paymentAvroSerde) /* right value */
		);

		joined.to("PayedPurchases");

		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.cleanUp();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

}
