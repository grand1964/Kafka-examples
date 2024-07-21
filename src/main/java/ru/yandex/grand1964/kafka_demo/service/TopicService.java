package ru.yandex.grand1964.kafka_demo.service;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Service
public class TopicService {
    @Autowired
    private final KafkaAdmin kafkaAdmin;

    public TopicService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    public void topic(String topicName, int partitionCount, short replicaCount) {
        NewTopic newTopic = TopicBuilder.name(topicName)
                .partitions(partitionCount)
                .replicas(replicaCount)
                .config(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.toString())
                .build();
        kafkaAdmin.createOrModifyTopics(newTopic);
    }

    /*public void topic(String topicName, int partitionCount, short replicaCount) {
        //создаем AdminClient, читая конфигурацию из kafkaAdmin
        try (Admin admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
            //получаем существующие темы у администратора
            ListTopicsResult listTopics = admin.listTopics();
            try { //метод get объявляет проверяемые исключения
                Set<String> names = listTopics.names().get(); //множество имен тем
                if (!names.contains(topicName)) { //темы еще не было
                    //создаем новую тему (средствами Spring; можно использовать также объект NewTopic из Kafka)
                    NewTopic newTopic = TopicBuilder.name(topicName)
                            .partitions(partitionCount)
                            .replicas(replicaCount)
                            .config(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.toString())
                            .build();
                    //добавляем ее в коллекцию тем
                    CreateTopicsResult result = admin.createTopics(Collections.singleton(newTopic));
                    //получаем KafkaFuture<Void>, соответствующее новой теме
                    KafkaFuture<Void> future = result.values().get(topicName);
                    //ждем результат
                    future.get();
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Bad topic names");
            }
        }
    }*/
}
