package ru.yandex.grand1964.kafka_demo.controller;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping
public class MsgController {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaAdmin kafkaAdmin;

    @Autowired
    public MsgController(KafkaTemplate<String, Object> kafkaTemplate, KafkaAdmin kafkaAdmin) {
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

    //посылка полных данных в Kafka
    @PostMapping("/send-full")
    public void sendMsg(@RequestParam String topic, @RequestParam String key, @RequestBody StatInDto dto){
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, dto);
        future.whenComplete((ok,ex) -> {
            if (ok != null) {
                System.out.println(ok);
            } else {
                System.err.println("Error: " + ex.getMessage());
            }
        });
        kafkaTemplate.flush();
    }


    //посылка частичных данных в Kafka
    @PostMapping("/send-part")
    public void sendMsg(@RequestBody StatInDto dto){
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(
                dto.getApp(), dto.getUri(), new StatPartDto(dto));
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
