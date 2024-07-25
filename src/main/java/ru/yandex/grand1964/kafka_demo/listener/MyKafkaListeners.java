package ru.yandex.grand1964.kafka_demo.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Service
public class MyKafkaListeners {
    /*@KafkaListener(groupId = "${spring.kafka.consumer.group-id}", topics = MAIN_TOPIC,
            containerFactory = "inKafkaListenerContainerFactory")
    @SendTo(TOPIC_PREFIX + "!{request.value().getApp()}")*/
    //@KafkaListener(groupId = "${spring.kafka.consumer.group-id}", topics = "${main.topic.name}",
    @KafkaListener(groupId = "${consumer.main.group-id}", topics = "${main.topic.name}", clientIdPrefix = "full",
            containerFactory = "inKafkaListenerContainerFactory")
    //@SendTo(TOPIC_PREFIX + "!{request.value().getApp()}")
    @SendTo("${topic.prefix}!{request.value().getApp()}")
    public Message<StatPartDto> handleFullStat(@Payload StatInDto statInDto,
                                               @Headers MessageHeaders headers) {
        System.out.println("THAT IS MSG-CONSUMER!!!!!!!!!");
        System.out.println(headers);

        //TODO Версия с DateTimeFormatter
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.of("GMT+3"));
        //long dateMillis = ((LocalDateTime) formatter.parse(statInDto.getTimestamp()))
        long dateMillis = LocalDateTime.parse(statInDto.getTimestamp(), formatter)
                .toInstant(ZoneOffset.of("+03:00"))
                .toEpochMilli();

        /*DateFormatter formatter = new DateFormatter("yyyy-MM-dd HH:mm:ss");
        long dateMillis;
        try {
            Locale rus = new Locale.Builder().setLanguage("ru").setRegion("RU").build();
            dateMillis = formatter.parse(statInDto.getTimestamp(), rus).toInstant().toEpochMilli();
        } catch (ParseException e) {
            throw new RuntimeException("Bad pattern");
        }*/
        return MessageBuilder.withPayload(new StatPartDto(statInDto))
                //.setHeader(KafkaHeaders.TOPIC, statInDto.getApp())
                .setHeader(KafkaHeaders.KEY, statInDto.getUri())
                //.setHeader(KafkaHeaders.TIMESTAMP_TYPE, TimestampType.CREATE_TIME)
                .setHeader(KafkaHeaders.TIMESTAMP, dateMillis)
                .build();
    }

    //TODO Проверить шаблоны!!!
    //@KafkaListener(groupId = "${spring.kafka.consumer.group-id}", topics = "ewm-main-service",
    //@KafkaListener(groupId = "${spring.kafka.consumer.group-id}", topics = "#{TOPIC_PREFIX + \".*\"}",
    @KafkaListener(groupId = "${consumer.client.group-id}",
            topicPattern = "${topic.prefix}.*", clientIdPrefix = "part",
    //@KafkaListener(groupId = "${kafka.consumer.my.group-id:app.client}", topicPattern = "${topic.prefix}.*",
    //@KafkaListener(groupId = "app.2", topicPattern = TOPIC_PREFIX + ".*",
    //@KafkaListener(groupId = "${spring.kafka.consumer.group-id}", topicPattern = TOPIC_PREFIX + ".*",
            //containerFactory = "outKafkaListenerContainerFactory")
            containerFactory = "outKafkaListenerContainerFactory", properties = "metadata.max.age.ms:1000")
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