package ru.yandex.grand1964.kafka_demo.controller;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.bind.annotation.*;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;

import java.time.Instant;
import java.util.HashMap;
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

    //посылка полных данных в Kafka
    @PostMapping("/send-kv")
    //public void sendKeyValue(@RequestParam String topic, @RequestParam String key, @RequestBody StatInDto dto){
    public void sendKeyValue(@RequestParam String topic, @RequestBody StatInDto dto){
        CompletableFuture<SendResult<String, StatInDto>> future = kafkaTemplate.send(topic, dto.getUri(), dto);
        //CompletableFuture<SendResult<String, StatInDto>> future = kafkaTemplate.send(topic, key, dto);
        future.whenComplete((ok,ex) -> {
            if (ok != null) {
                System.out.println(ok);
            } else {
                System.err.println("Error: " + ex.getMessage());
            }
        });
        kafkaTemplate.flush();
    }

    @PostMapping("/send-record")
    public void sendRecord(@RequestParam String topic, @RequestBody StatInDto dto){
    //public void sendRecord(@RequestParam String topic, @RequestParam String key, @RequestBody StatInDto dto){
        ProducerRecord<String, StatInDto> producerRecord =
                new ProducerRecord<>(topic,0, Instant.now().toEpochMilli(), dto.getUri(), dto);
                //new ProducerRecord<>(topic,0, Instant.now().toEpochMilli(), key, dto);
        CompletableFuture<SendResult<String, StatInDto>> future = kafkaTemplate.send(producerRecord);
        future.whenComplete((ok,ex) -> {
            if (ok != null) {
                System.out.println(ok);
            } else {
                System.err.println("Error: " + ex.getMessage());
            }
        });
        kafkaTemplate.flush();
    }

    @PostMapping("/send-message")
    //public void sendMessage(@RequestParam String topic, @RequestParam String key, @RequestBody StatInDto dto){
    public void sendMessage(@RequestParam String topic, @RequestBody StatInDto dto){
        //создаем заголовки
        HashMap<String, Object> headersMap = new HashMap<>();
        headersMap.put(KafkaHeaders.TOPIC, topic);
        headersMap.put(KafkaHeaders.PARTITION, 0);
        headersMap.put(KafkaHeaders.TIMESTAMP, Instant.now().toEpochMilli());
        //headersMap.put(KafkaHeaders.KEY, dto.getUri());
        headersMap.put("FORWARDED_KEY", dto.getUri());
        MessageHeaders messageHeaders = new MessageHeaders(headersMap);
        //создаем сообщение с заголовками
        Message<StatInDto> message = new GenericMessage<>(dto, messageHeaders);
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


    //посылка частичных данных в Kafka
    /*@PostMapping("/send-part")
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
    }*/
}
