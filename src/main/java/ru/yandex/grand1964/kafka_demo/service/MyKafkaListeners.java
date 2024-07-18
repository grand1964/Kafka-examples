package ru.yandex.grand1964.kafka_demo.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

@Component
public class MyKafkaListeners {
    @KafkaListener(groupId = "${spring.kafka.consumer.group-id}", topics = "msg",
            containerFactory = "inKafkaListenerContainerFactory")
    @SendTo("!{request.value().getApp()}")
    public Message<StatPartDto> handleFullStat(@Payload StatInDto statInDto) {
        return MessageBuilder.withPayload(new StatPartDto(statInDto))
                .setHeader(KafkaHeaders.TOPIC, statInDto.getApp())
                .setHeader(KafkaHeaders.KEY, statInDto.getUri())
                .build();

    }

    @KafkaListener(groupId = "${spring.kafka.consumer.group-id}", topics = "ewm-main-service",
            containerFactory = "outKafkaListenerContainerFactory")
    public void handlePartStat(ConsumerRecord<String, StatPartDto> record) {
        System.out.println("Partial stat received: ");
        System.out.println("Payload: " + record.value());
        System.out.println("Key: " + record.key());
        System.out.println("topic: " + record.topic());
        System.out.println("partition: " + record.partition());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ss");
        System.out.println("Timestamp: " +
                formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()),
                        ZoneId.of("GMT+3"))));
    }
}