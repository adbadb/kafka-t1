package kafkaex;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalConsumer {

	private static final Logger LOG = LoggerFactory.getLogger(TransactionalConsumer.class);

	private static final String BOOTSTRAP_SERVERS = ":9092";
	private static final String GROUP_ID = "ex34";
	private static final String OFFSET_RESET = "earliest";

	@SuppressWarnings("boxing")
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(Collections.singleton(args[0]));
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
				for (ConsumerRecord<String, String> data : records) {
					LOG.info("key = {}, value = {} => partition = {}, offset= {}", data.key(), data.value(), data.partition(), data.offset());
				}
			}
		} catch (Exception e) {
			LOG.error("Something goes wrong: {}", e.getMessage(), e);
		}
	}
}
