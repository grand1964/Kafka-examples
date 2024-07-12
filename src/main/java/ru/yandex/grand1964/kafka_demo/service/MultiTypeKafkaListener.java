package ru.yandex.grand1964.kafka_demo.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

@Component
public class MultiTypeKafkaListener {
    @KafkaListener(topics = "msg")
    /*public void handleFullStat(@Payload StatInDto statInDto) {
        System.out.println("Full stat received: ");
        System.out.println("Payload: " + statInDto);*/
    public void handleFullStat(ConsumerRecord<String,StatInDto> consumerRecord) {
        System.out.println("Full stat received: ");
        System.out.println("Key: " + consumerRecord.key());
        System.out.println("Payload: " + consumerRecord.value());
    }

    @KafkaListener(topics = "ewm-main-service")
    public void handlePartStat(ConsumerRecord<String,StatPartDto> consumerRecord) {
        System.out.println("Partial stat received: ");
        System.out.println("Key: " + consumerRecord.key());
        System.out.println("Payload: " + consumerRecord.value());
    }
}
