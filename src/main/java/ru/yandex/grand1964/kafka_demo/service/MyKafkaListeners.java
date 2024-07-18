package ru.yandex.grand1964.kafka_demo.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Component
public class MyKafkaListeners {
    @KafkaListener(groupId = "${spring.kafka.consumer.group-id}", topics = "msg",
            containerFactory = "inKafkaListenerContainerFactory")
    @SendTo("!{request.value().getApp()}")
    public StatPartDto handleFullStat(@Payload StatInDto statInDto,
                                      @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                                      @Header(KafkaHeaders.RECEIVED_PARTITION) String partition,
                                      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                      @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ss");
        System.out.println("Full stat received: ");
        System.out.println("Payload: " + statInDto);
        System.out.println("Key: " + key);
        System.out.println("topic: " + topic);
        System.out.println("partition: " + partition);
        System.out.println("Timestamp: " +
                formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(ts),
                        ZoneId.of("GMT+3"))));
        return new StatPartDto(statInDto);
    }

    @KafkaListener(groupId = "app.1", topics = "ewm-main-service",
            containerFactory = "outKafkaListenerContainerFactory")
    public void handlePartStat(ConsumerRecord<String, StatPartDto> record) {
        System.out.println("Partial stat received: ");
        System.out.println("Payload: " + record.value());
        System.out.println("Key: " + record.key());
        System.out.println("topic: " + record.topic());
        //System.out.println("Reply topic: " + replyTopic);
        System.out.println("partition: " + record.partition());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ss");
        System.out.println("Timestamp: " +
                formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()),
                        ZoneId.of("GMT+3"))));
    }
}