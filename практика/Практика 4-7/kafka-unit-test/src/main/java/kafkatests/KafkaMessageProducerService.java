package kafkatests;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageProducerService {

    private static final String TOPIC = "topic1";
    
	private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public KafkaMessageProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String msg) {
        kafkaTemplate.send(TOPIC, msg);
    }
}