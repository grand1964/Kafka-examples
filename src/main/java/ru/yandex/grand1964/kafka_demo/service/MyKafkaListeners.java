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
        //TODO Версия с чистым Payload
    /*public StatPartDto handleFullStat(@Payload StatInDto statInDto,
                                              //@Header(name = "FORWARDED_KEY", required = false) String fkey,
                                              @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                                              @Header(KafkaHeaders.RECEIVED_PARTITION) String partition,
                                              @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                              @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ss");
        //ПОСМОТРЕТЬ ТАКЖЕ DateFormatter ИЗ Spring!!!
        System.out.println("Full stat received: ");
        System.out.println("Payload: " + statInDto);
        System.out.println("Key: " + key);*/

        /*System.out.println("Timestamp: " +
                formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("GMT+3"))));
        System.out.println("Headers reseived:");
        System.out.println("key: " + key);
        System.out.println("partition: " + partition);
        System.out.println("topic: " + topic);

        return new StatPartDto(statInDto);*/

        //TODO Конец версии с чистым Payload

    /*public void handleFullStat(ConsumerRecord<String,StatInDto> consumerRecord) {
        System.out.println("Full stat received: ");
        System.out.println("Key: " + consumerRecord.key());
        System.out.println("Payload: " + consumerRecord.value());*/
    }

    /*@KafkaListener(groupId = "app.1", topics = "ewm-main-service",
            containerFactory = "outKafkaListenerContainerFactory")
    public void handlePartStat(@Payload StatPartDto value, ConsumerRecordMetadata metadata,
                               //@Header("FORWARDED_KEY") String key) {
                               @Header("KafkaHeaders.RECEIVED_KEY") String key) {
        System.out.println("Partial stat received: ");
        System.out.println("Payload: " + value);
        System.out.println("Key: " + key);
        System.out.println("Headers reseived:");
        System.out.println("topic: " + metadata.topic());
        System.out.println("partition: " + metadata.partition());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ss");
        System.out.println("Timestamp: " +
                formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(metadata.timestamp()),
                        ZoneId.of("GMT+3"))));
    }*/

    @KafkaListener(groupId = "app.1", topics = "ewm-main-service",
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

    /*public void handlePartStat(ConsumerRecord<String,StatPartDto> consumerRecord) {
        System.out.println("Partial stat received: ");
        System.out.println("Key: " + consumerRecord.key());
        System.out.println("Payload: " + consumerRecord.value());
    }*/
}