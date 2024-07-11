package ru.yandex.grand1964.kafka_demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;

@SpringBootApplication
public class KafkaDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaDemoApplication.class, args);
	}

	@KafkaListener(topics = "msg")
	public void handleFullStat(ConsumerRecord<String,StatInDto> consumerRecord) {
	//public void handleFullStat(@Payload StatInDto statInDto) {
	//public void handleFullStat(String statInDto) {
		System.out.println("Full stat received: ");
		System.out.println("Payload: " + consumerRecord.value());
		System.out.println("Key: " + consumerRecord.key());
	}

}
