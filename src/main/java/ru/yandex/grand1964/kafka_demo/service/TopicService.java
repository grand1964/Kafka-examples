package ru.yandex.grand1964.kafka_demo.service;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
    @Value("${topic.prefix}")
    private String TOPIC_PREFIX;

    public TopicService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    public void topic(String topicName, int partitionCount, short replicaCount) {
        NewTopic newTopic = TopicBuilder.name(TOPIC_PREFIX + topicName)
                .partitions(partitionCount)
                .replicas(replicaCount)
                .config(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.toString())
                .build();
        kafkaAdmin.createOrModifyTopics(newTopic);
    }
}
