package cl.wom.poc;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

	/**
	 * Log de la clase.
	 */
	private static Logger log = LogManager.getLogger(WomSpringbootKafkaMinConsumerApplication.class);

	public static final String TOPIC = "poc-test01";

	public static final String GROUP_ID = "myGroupId04";

	public static void main(String[] args) {
		SpringApplication.run(WomSpringbootKafkaMinConsumerApplication.class, args);
	}

	@Bean
	public NewTopic topic() {
		return TopicBuilder.name(TOPIC).partitions(10).replicas(1).build();
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
		// consumerProps.put("group.id", String.format(GROUP_ID));

		// (boolean - default true)
		// https://kafka.apache.org/documentation/#consumerconfigs_enable.auto.commit
		consumerProps.put("enable.auto.commit", false);

		// Session Timeout (int - default: 45000 (45 seconds))
		// https://kafka.apache.org/documentation/#consumerconfigs_session.timeout.ms
		// consumerProps.put("session.timeout.ms", 45000);

		// Heartbeat Intervals (int - default: 3000 (3 seconds))
		// https://kafka.apache.org/documentation/#consumerconfigs_heartbeat.interval.ms
		//consumerProps.put("heartbeat.interval.ms", 1000);

		// Default Api Timeout (int - default 60000 (1 minute))
		// https://kafka.apache.org/documentation/#consumerconfigs_default.api.timeout.ms
		// consumerProps.put("default.api.timeout.ms", 60000);

		// Request Timeout (int - default 40000 (40 seconds))
		// https://kafka.apache.org/documentation/#connectconfigs_request.timeout.ms
		//consumerProps.put("request.timeout.ms", 130000);

		// Max Poll Interval (int - default: 300000 (5 minutes))
		// https://kafka.apache.org/documentation/#consumerconfigs_max.poll.interval.ms
		// consumerProps.put("max.poll.interval.ms", 300000);

		// Max Poll Records (int - default: 500)
		// https://kafka.apache.org/documentation/#consumerconfigs_max.poll.records
		consumerProps.put("max.poll.records", 2);

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

	@KafkaListener(id = GROUP_ID, topics = TOPIC, containerFactory = "kafkaListenerContainerFactory")
	public void listen(ConsumerRecord<String, String> message, Acknowledgment ack) {

		log.info("[listen] value = {}, Partition = {}, Offset = {}", message.value(), message.partition(),
				message.offset());

		try {
			Thread.sleep(120000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		ack.acknowledge();
	}

}
