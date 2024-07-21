package ru.yandex.grand1964.kafka_demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.*;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.service.TopicService;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping
public class MsgController {
    private final KafkaTemplate<String, StatInDto> kafkaTemplate;
    //private final KafkaAdmin kafkaAdmin;
    private final TopicService topicService;

    @Autowired
    public MsgController(KafkaTemplate<String, StatInDto> kafkaTemplate,
                         //KafkaAdmin kafkaAdmin,
                         TopicService topicService) {
        this.kafkaTemplate = kafkaTemplate;
        //this.kafkaAdmin = kafkaAdmin;
        //kafkaTemplate.setKafkaAdmin(kafkaAdmin);
        this.topicService = topicService;
    }

    //создание новой темы
    @PostMapping("/topic/{topicName}")
    public void createTopic(@PathVariable String topicName,
                            @RequestParam(defaultValue = "1") int partitionCount,
                            @RequestParam(defaultValue = "1") short replicaCount) {
        topicService.topic(topicName, partitionCount, replicaCount);
        /*NewTopic newTopic = TopicBuilder.name(topicName)
                .partitions(partitionCount)
                .replicas(replicaCount)
                .config(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.toString())
                .build();
        kafkaAdmin.createOrModifyTopics(newTopic);*/
    }

    //посылка полных данных в Kafka в формате message
    @PostMapping("/send-message")
    //public void sendMessage(@RequestParam String topic, @RequestParam String key, @RequestBody StatInDto dto){
    public void sendMessage(@RequestParam String topic, @RequestBody StatInDto dto) {
        Message<StatInDto> message = MessageBuilder.withPayload(dto)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.PARTITION, 0)
                .setHeader(KafkaHeaders.TIMESTAMP, Instant.now().minusMillis(1234567L).toEpochMilli())
                //.setHeader(KafkaHeaders.TIMESTAMP_TYPE, TimestampType.LOG_APPEND_TIME)
                .setHeader(KafkaHeaders.KEY, dto.getUri())
                .build();
        //TODO Ввести конвертацию времени из Payload
        //посылаем сообщение
        CompletableFuture<SendResult<String, StatInDto>> future = kafkaTemplate.send(message);
        future.whenComplete((ok,ex) -> {
            if (ok != null) {
                System.out.println(ok);
            } else {
                System.err.println("Error: " + ex.getMessage());
            }
        });
        kafkaTemplate.flush();
    }
}
