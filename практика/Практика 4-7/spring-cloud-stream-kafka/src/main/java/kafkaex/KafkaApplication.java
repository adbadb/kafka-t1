package kafkaex;

import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@SuppressWarnings("static-method")
public class KafkaApplication {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaApplication.class);

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

	@Bean
	public Supplier<String> send() {
		return () -> String.valueOf(new Random().nextInt());
	}

	@Bean
	public Consumer<String> receive() {
		return KafkaApplication::log;
	}

	private static void log(String message) {
		LOG.info("Message {}", message);
	}
}
