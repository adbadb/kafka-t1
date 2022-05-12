package kafkatests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest
@EmbeddedKafka(partitions = 5, topics = { "topic1" })
class KafkaTest {

	@Test
	void contextLoads() {
		// nothing
	}

}
