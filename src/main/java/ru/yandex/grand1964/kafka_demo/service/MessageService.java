package ru.yandex.grand1964.kafka_demo.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Service
public class MessageService {
    //private final TopicService topicService;
    @Value("${time.format.pattern}")
    private String timePattern;
    @Value("${topic.prefix}")
    private String topicPrefix;

    /*@Autowired
    public MessageService(TopicService topicService) {
        this.topicService = topicService;
    }*/

    public Message<StatPartDto> fromStatInDto(StatInDto statInDto) {
        //имя темы - это имя приложения с префиксом
        String topicName = topicPrefix + statInDto.getApp();
        //генерируем dto без имени приложения
        StatPartDto statPartDto = statInDto.toPartDto();
        //преобразуем время из вложения в UNIX-формат
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timePattern);
        long dateMillis = LocalDateTime.parse(statInDto.getTimestamp(), formatter)
                .toInstant(ZoneOffset.of("+03:00"))
                .toEpochMilli();
        //Message<StatPartDto> message = MessageBuilder.withPayload(statPartDto)
        return MessageBuilder.withPayload(statPartDto)
                .setHeader(KafkaHeaders.TOPIC, topicName)
                .setHeader(KafkaHeaders.KEY, statInDto.getUri())
                //TODO ВЕРНУТЬ !!!!!!!!!!!!!!
                //.setHeader(KafkaHeaders.TIMESTAMP, dateMillis)
                .setHeader(KafkaHeaders.PARTITION, 0)
                //время назначает система
                .build();
    }
}
