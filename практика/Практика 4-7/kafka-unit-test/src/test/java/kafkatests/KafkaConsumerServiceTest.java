package kafkatests;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Map;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
@EmbeddedKafka(partitions = 5, topics = { "topic2" })
class KafkaConsumerServiceTest {

	@SpyBean
	private KafkaMessageConsumerService consumerService;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	@Captor
	private ArgumentCaptor<String> payLoadArgumentCaptor;

	@Captor
	private ArgumentCaptor<String> topicArgumentCaptor;

	@Captor
	private ArgumentCaptor<Integer> partitionArgumentCaptor;

	@Captor
	private ArgumentCaptor<Long> offsetArgumentCaptor;

	private Producer<String, String> producer;

	@BeforeEach
	public void setUp() {
		Map<String, Object> configs = KafkaTestUtils.producerProps(embeddedKafkaBroker);
		producer = new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new StringSerializer())
			.createProducer();
	}

	@Test
	void shouldReadMessage() {

		producer.send(new ProducerRecord<>("topic2", 0, null, "msg2"));
		producer.flush();

		verify(consumerService, timeout(50000).times(1)).onMessage(payLoadArgumentCaptor.capture(),
			topicArgumentCaptor.capture(), partitionArgumentCaptor.capture(), offsetArgumentCaptor.capture());

		assertEquals("msg2", payLoadArgumentCaptor.getValue());
		assertEquals("topic2", topicArgumentCaptor.getValue());
		assertEquals(0, partitionArgumentCaptor.getValue());
		assertEquals(0, offsetArgumentCaptor.getValue());
	}

	@AfterEach
	void shutdown() {
		producer.close();
	}
}