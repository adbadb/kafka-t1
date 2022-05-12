package com.luxoft.eas026.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class UpperCaseProcessorExample {

	public static void main(final String[] args) throws Exception {
		final Properties streamsConfiguration = new Properties();

		final Serde<String> stringSerde = Serdes.String();

		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-example");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "processor-example-client");

		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());

	
		final Topology topology = new Topology();
		topology.addSource("source-node", stringSerde.deserializer(), stringSerde.deserializer(), "TextLinesTopic");

		topology.addProcessor("uppercase-node",new ProcessorSupplier<String, String, String, String>() {

			@Override
			public Processor<String, String, String, String> get() {
				return new UpperCaseProcessor();
			}
		}, "source-node");

		topology.addSink("sink-node", "UpperTextLineProcessor", stringSerde.serializer(), stringSerde.serializer(),
				"uppercase-node");

		System.out.println(topology.describe());
		final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
		streams.cleanUp();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}
}
