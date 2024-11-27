package ru.yandex.grand1964.kafka_demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.*;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;
import ru.yandex.grand1964.kafka_demo.service.MessageService;
import ru.yandex.grand1964.kafka_demo.service.TopicService;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping
public class MsgController {
    @Autowired
    @Qualifier("&statStreamsBuilder")
    private StreamsBuilderFactoryBean factory;
    @Value("${topic.prefix}")
    private String topicPrefix;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TopicService topicService;
    private final MessageService messageService;

    @Autowired
    public MsgController(KafkaTemplate<String, Object> kafkaTemplate,
                         TopicService topicService, MessageService messageService) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicService = topicService;
        this.messageService = messageService;
    }

    //создание новой темы
    @PostMapping("/topic/{topicName}")
    public void createTopic(@PathVariable String topicName,
                            @RequestParam(defaultValue = "1") int partitionCount,
                            @RequestParam(defaultValue = "1") short replicaCount) {
        //создание темы - в службе тем
        topicService.topic(topicName, partitionCount, replicaCount);
    }

    //TODO СТАРАЯ ВЕРСИЯ
    /*//посылка полных данных в Kafka в формате message БЕЗ КЛЮЧА
    @PostMapping("/send-message")
    public void sendMessage(@RequestParam String topic, @RequestBody StatInDto dto) {
        //создаем тему, соответствующую приложению (если ее еще нет)
        topicService.topic(dto.getApp(),1, (short) 1);
        //создаем сообщение (время назначается сервером)
        Message<StatInDto> message = MessageBuilder.withPayload(dto)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.PARTITION, 0)
                //других заголовков не надо: время назначает система, а ключ не нужен
                .build();
        //посылаем сообщение
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(message);
        future.whenComplete((ok, ex) -> {
            if (ok != null) {
                System.out.println(ok);
            } else {
                System.err.println("Error: " + ex.getMessage());
            }
        });
        kafkaTemplate.flush();
    }*/

    //посылка полных данных в Kafka в формате message
    @PostMapping("/send-message")
    public void sendMessage(@RequestBody StatInDto dto) {
        //создаем тему, соответствующую приложению (если ее еще нет)
        topicService.topic(topicPrefix + dto.getApp(),1, (short) 1);
        //создаем сообщение
        Message<StatPartDto> message = messageService.fromStatInDto(dto);
        //посылаем сообщение
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(message);
        future.whenComplete((ok, ex) -> {
            if (ok != null) {
                System.out.println(ok);
            } else {
                System.err.println("Error: " + ex.getMessage());
            }
        });
        kafkaTemplate.flush();
    }

    @PostMapping("/start-stream")
    public void startStream() {
        //TODO Редактировать
        topicService.topic("common", 1, (short) 1);
        topicService.topic("sink", 1, (short) 1);
        factory.start();
    }

    @PostMapping("/stop-stream")
    public void stopStream() {
        factory.stop();
    }
}