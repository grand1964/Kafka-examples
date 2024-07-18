package ru.yandex.grand1964.kafka_demo.config;

import jakarta.annotation.Nonnull;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Configuration
public class KafkaProducerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;

    /////////////////////////// Параметры продюсера //////////////////////////

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class);
        return props;
    }

    ////////////////////////// Основная конфигурация /////////////////////////

    @Bean
    public ProducerFactory<String, StatInDto> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, StatInDto> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    ////////////////////////// Конфигурация отклика //////////////////////////

    @Bean
    public ProducerFactory<String, StatPartDto> replyingProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, StatPartDto> replyingKafkaTemplate() {
        return new KafkaTemplate<>(replyingProducerFactory()) { //переопределяем продюсера
            //посылка сообщения с преобразованием данных
            @Override
            @Nonnull
            public CompletableFuture<SendResult<String, StatPartDto>> send(@Nonnull Message<?> message) {
                StatPartDto dto = (StatPartDto) message.getPayload();
                MessageHeaders headers = message.getHeaders();
                Message<StatPartDto> replyMessage = MessageBuilder.withPayload(dto)
                        .setHeader(KafkaHeaders.TOPIC, headers.get(KafkaHeaders.TOPIC))
                        .setHeader(KafkaHeaders.PARTITION, 0)
                        .setHeader(KafkaHeaders.KEY, dto.getUri())
                        .build();
                return super.send(replyMessage);
            }
        };
    }
}
