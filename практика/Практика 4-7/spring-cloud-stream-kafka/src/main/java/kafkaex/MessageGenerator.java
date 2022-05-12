package kafkaex;

import java.util.Random;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MessageGenerator {

	@Autowired
	private StreamBridge streamBridge;

	@Scheduled(cron = "*/5 * * * * *")
	public void sendMessage() {
		streamBridge.send("send-out-0", "MyTestMessage " + new Random().nextInt());
	}
}