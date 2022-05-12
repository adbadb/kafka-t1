package com.luxoft.eas026.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class MapFunctionLambdaExample {

	public static void main(final String[] args) {
	
		final Properties streamsConfiguration = new Properties();
	
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-lambda-example");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "map-function-lambda-example-client");
		
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,  "localhost:9092");
	
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		final StreamsBuilder builder = new StreamsBuilder();

		final KStream<String, String> textLines = builder.stream("TextLinesTopic");

		final KStream<String, String> uppercasedWithMapValues = textLines.mapValues(v -> v.toUpperCase());

		uppercasedWithMapValues.to("UppercasedTextLinesTopic");

		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.cleanUp();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

}
