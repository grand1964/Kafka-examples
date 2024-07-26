package ru.yandex.grand1964.kafka_demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.datetime.DateFormatter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.*;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.service.TopicService;

import java.text.ParseException;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping
public class MsgController {
    private final KafkaTemplate<String, StatInDto> kafkaTemplate;
    private final TopicService topicService;

    @Autowired
    public MsgController(KafkaTemplate<String, StatInDto> kafkaTemplate,
                         TopicService topicService) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicService = topicService;
    }

    //создание новой темы
    @PostMapping("/topic/{topicName}")
    public void createTopic(@PathVariable String topicName,
                            @RequestParam(defaultValue = "1") int partitionCount,
                            @RequestParam(defaultValue = "1") short replicaCount) {
        //создание темы - в службе тем
        topicService.topic(topicName, partitionCount, replicaCount);
    }

    //посылка полных данных в Kafka в формате message БЕЗ КЛЮЧА
    @PostMapping("/send-message")
    public void sendMessage(@RequestParam String topic, @RequestBody StatInDto dto) {
        //создаем тему, соответствующую приложению (если ее еще нет)
        topicService.topic(dto.getApp(),1, (short) 1);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException("xxx");
        }
        //создаем сообщение (время назначается сервером)
        Message<StatInDto> message = MessageBuilder.withPayload(dto)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.PARTITION, 0)
                //других заголовков не надо: время назначает система, а ключ не нужен
                .build();
        //посылаем сообщение
        CompletableFuture<SendResult<String, StatInDto>> future = kafkaTemplate.send(message);
        future.whenComplete((ok, ex) -> {
            if (ok != null) {
                System.out.println(ok);
            } else {
                System.err.println("Error: " + ex.getMessage());
            }
        });
        kafkaTemplate.flush();
    }
}
