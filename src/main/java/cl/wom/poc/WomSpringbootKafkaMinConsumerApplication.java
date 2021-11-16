package cl.wom.poc;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;

@SpringBootApplication
public class WomSpringbootKafkaMinConsumerApplication {

	public static final String TOPIC = "poc-test01";

	public static final String GROUP_ID = "myGroupId01";

	public static void main(String[] args) {
		SpringApplication.run(WomSpringbootKafkaMinConsumerApplication.class, args);
	}

	@Bean
	public NewTopic topic() {
		return TopicBuilder.name("topic1").partitions(10).replicas(1).build();
	}

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {

		Map<String, Object> consumerProps = new HashMap<>();

		// Kafka broker
		consumerProps.put("bootstrap.servers", "localhost:9092");

		// Deserialization
		consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		// Kafka broker
		consumerProps.put("group.id", GROUP_ID);

		// (boolean - default true)
		// https://kafka.apache.org/documentation/#consumerconfigs_enable.auto.commit
		consumerProps.put("enable.auto.commit", false);

		// (int - default 60000 (1 minute))
		// https://kafka.apache.org/documentation/#consumerconfigs_default.api.timeout.ms
		consumerProps.put("default.api.timeout.ms", 500);

		// (int - default 40000 (40 seconds))
		// https://kafka.apache.org/documentation/#connectconfigs_request.timeout.ms
		consumerProps.put("request.timeout.ms", 40000);

		return new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer());

	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		factory.setConsumerFactory(consumerFactory());
		factory.setConcurrency(10);
		return factory;
	}

	@KafkaListener(id = "myId", topics = TOPIC, containerFactory = "kafkaListenerContainerFactory")
	public void listen(ConsumerRecord<String, String> message, Acknowledgment ack) {

		System.out.println("value = " + message.value() + ", Partition = " + message.partition() + ", Offset = "
				+ message.offset());

		try {
			Thread.sleep(120000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		ack.acknowledge();
	}

//	public void listen(String in, Acknowledgment ack) {
//		System.out.println("value = " + in);
//		try {
//			Thread.sleep(30000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		System.out.println("value = " + in + " - ACK");
//		ack.acknowledge();
//	}

}
