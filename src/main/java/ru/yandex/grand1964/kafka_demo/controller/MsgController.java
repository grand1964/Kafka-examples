package ru.yandex.grand1964.kafka_demo.controller;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;

import java.util.concurrent.CompletableFuture;

@RestController
//@RequestMapping("msg")
@RequestMapping
public class MsgController {
    private final KafkaTemplate<String, StatInDto> kafkaTemplate;
    private final KafkaAdmin kafkaAdmin;

    @Autowired
    public MsgController(KafkaTemplate<String, StatInDto> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaAdmin = kafkaTemplate.getKafkaAdmin();
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

    //посылка данных в Kafka
    @PostMapping("/send")
    public void sendMsg(@RequestParam String topic, @RequestParam String key, @RequestBody StatInDto dto){
        CompletableFuture<SendResult<String, StatInDto>> future = kafkaTemplate.send(topic, key, dto);
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
