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

import java.time.*;
import java.time.format.DateTimeFormatter;

@Service
public class MyKafkaListeners {
    @KafkaListener(groupId = "${consumer.main.group-id}", topics = "${main.topic.name}", clientIdPrefix = "full",
            containerFactory = "inKafkaListenerContainerFactory")
    @SendTo("${topic.prefix}!{request.value().getApp()}")
    public Message<StatPartDto> handleFullStat(@Payload StatInDto statInDto,
                                               @Headers MessageHeaders headers) {
        System.out.println("THAT IS COMMON-CONSUMER!!!!!!!!!");
        System.out.println(headers);
        //преобразуем время из вложения в UNIX-формат
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        long dateMillis = LocalDateTime.parse(statInDto.getTimestamp(), formatter)
                .toInstant(ZoneOffset.of("+03:00"))
                .toEpochMilli();
        return MessageBuilder.withPayload(new StatPartDto(statInDto))
                //установить ключ (по умолчанию он null)
                .setHeader(KafkaHeaders.KEY, statInDto.getUri())
                //заголовок важен, иначе timestamp установит система (у нас TIMESTAMP_TYPE=TimestampType.CREATE_TIME)
                .setHeader(KafkaHeaders.TIMESTAMP, dateMillis)
                .build();
    }

    @KafkaListener(groupId = "${consumer.client.group-id}",
            topicPattern = "${topic.prefix}.*", clientIdPrefix = "part",
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
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()),
                ZoneOffset.of("+03:00"));
                //ZoneId.of("UTC+3"));
        System.out.println("Timestamp: " + dateTime.format(formatter));
    }
}