package ru.yandex.grand1964.kafka_demo.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Component
public class MyKafkaListeners {
    @KafkaListener(groupId = "${spring.kafka.consumer.group-id}", topics = "msg",
            containerFactory = "multiTypeKafkaListenerContainerFactory")
    public void handleFullStat(@Payload StatInDto statInDto,
                               @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                               @Header(KafkaHeaders.RECEIVED_PARTITION) String partition,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ss");
        //ПОСМОТРЕТЬ ТАКЖЕ DateFormatter ИЗ Spring!!!
        System.out.println("Full stat received: ");
        System.out.println("Payload: " + statInDto);
        System.out.println("Timestamp: " +
                formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("GMT+3"))));
        System.out.println("Headers reseived:");
        System.out.println("key: " + key);
        System.out.println("partition: " + partition);
        System.out.println("topic: " + topic);
    /*public void handleFullStat(ConsumerRecord<String,StatInDto> consumerRecord) {
        System.out.println("Full stat received: ");
        System.out.println("Key: " + consumerRecord.key());
        System.out.println("Payload: " + consumerRecord.value());*/
    }

    /*@KafkaListener(topics = "ewm-main-service")
    public void handlePartStat(ConsumerRecord<String,StatPartDto> consumerRecord) {
        System.out.println("Partial stat received: ");
        System.out.println("Key: " + consumerRecord.key());
        System.out.println("Payload: " + consumerRecord.value());
    }*/
}
