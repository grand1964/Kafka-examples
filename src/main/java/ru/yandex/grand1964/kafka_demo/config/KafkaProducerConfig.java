package ru.yandex.grand1964.kafka_demo.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class);
        /*props.put(JsonSerializer.TYPE_MAPPINGS,
                "full:ru.yandex.grand1964.kafka_demo.dto.StatInDto," +
                        "part:ru.yandex.grand1964.kafka_demo.dto.StatPartDto");*/
        return props;
    }

    @Bean
    //public ProducerFactory<String, Object> multiProducerFactory() {
    public ProducerFactory<String, StatInDto> multiProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    //public KafkaTemplate<String, Object> kafkaTemplate() {
    public KafkaTemplate<String, StatInDto> kafkaTemplate() {
        return new KafkaTemplate<>(multiProducerFactory());
    }
}
