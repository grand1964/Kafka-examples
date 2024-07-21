package ru.yandex.grand1964.kafka_demo.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.format.datetime.DateFormatter;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

@Component
public class MyKafkaListeners {
    @KafkaListener(groupId = "${spring.kafka.consumer.group-id}", topics = "msg",
            containerFactory = "inKafkaListenerContainerFactory")
    @SendTo("!{request.value().getApp()}")
    public Message<StatPartDto> handleFullStat(@Payload StatInDto statInDto,
                                               @Headers MessageHeaders headers) {
        System.out.println(headers);
        DateFormatter formatter = new DateFormatter("yyyy-MM-dd HH:mm:ss");
        long dateMillis;
        try {
            Locale rus = new Locale.Builder().setLanguage("ru").setRegion("RU").build();
            dateMillis = formatter.parse(statInDto.getTimestamp(), rus).toInstant().toEpochMilli();
        } catch (ParseException e) {
            throw new RuntimeException("Bad pattern");
        }
        return MessageBuilder.withPayload(new StatPartDto(statInDto))
                //.setHeader(KafkaHeaders.TOPIC, statInDto.getApp())
                .setHeader(KafkaHeaders.KEY, statInDto.getUri())
                //.setHeader(KafkaHeaders.TIMESTAMP_TYPE, TimestampType.CREATE_TIME)
                .setHeader(KafkaHeaders.TIMESTAMP, dateMillis)
                .build();
    }

    @KafkaListener(groupId = "${spring.kafka.consumer.group-id}", topics = "ewm-main-listener",
            containerFactory = "outKafkaListenerContainerFactory")
    public void handlePartStat(ConsumerRecord<String, StatPartDto> record,
                               @Headers MessageHeaders headers) {
        System.out.println(headers);
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