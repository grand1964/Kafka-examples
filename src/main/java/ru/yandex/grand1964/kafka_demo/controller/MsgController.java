package ru.yandex.grand1964.kafka_demo.controller;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.record.TimestampType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.*;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping
public class MsgController {
    private final KafkaTemplate<String, StatInDto> kafkaTemplate;
    private final KafkaAdmin kafkaAdmin;

    @Autowired
    public MsgController(KafkaTemplate<String, StatInDto> kafkaTemplate, KafkaAdmin kafkaAdmin) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaAdmin = kafkaAdmin;
        kafkaTemplate.setKafkaAdmin(kafkaAdmin);
    }

    //создание новой темы
    @PostMapping("/topic/{topicName}")
    public void createTopic(@PathVariable String topicName){
        NewTopic newTopic = TopicBuilder.name(topicName)
                .partitions(1)
                .replicas(1)
                .build();
        kafkaAdmin.createOrModifyTopics(newTopic);
    }

    //посылка полных данных в Kafka в формате message
    @PostMapping("/send-message")
    //public void sendMessage(@RequestParam String topic, @RequestParam String key, @RequestBody StatInDto dto){
    public void sendMessage(@RequestParam String topic, @RequestBody StatInDto dto) {
        Message<StatInDto> message = MessageBuilder.withPayload(dto)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.PARTITION, 0)
                .setHeader(KafkaHeaders.TIMESTAMP, Instant.now().toEpochMilli())
                .setHeader(KafkaHeaders.TIMESTAMP_TYPE, TimestampType.CREATE_TIME)
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
