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
import org.springframework.lang.NonNullApi;
import org.springframework.messaging.Message;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

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
        return props;
    }

    @Bean
    public ProducerFactory<String, StatInDto> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public ProducerFactory<String, StatPartDto> replyingProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, StatInDto> kafkaTemplate() {
        //return new KafkaTemplate<>(producerFactory());
        //TODO Убрать !!!!!!!!
        return new KafkaTemplate<>(producerFactory()) {
            @Override
            @Nonnull
            public CompletableFuture<SendResult<String, StatInDto>> send(@Nonnull Message<?> message) {
                return super.send(message);
            }
        };
    }

    @Bean
    public KafkaTemplate<String, StatPartDto> replyingKafkaTemplate() {
        return new KafkaTemplate<>(replyingProducerFactory()) {
            //@Override
            //public CompletableFuture<SendResult<String, StatPartDto>> send(String topic,
            //StatPartDto data) {
            //    return super.send(topic, 0, keyForData(data), data);
            //}

            @Override
            @Nonnull
            public CompletableFuture<SendResult<String, StatPartDto>> send(@Nonnull Message<?> message) {
                //TODO Версия с переопределением
                /*return super.send(topicForData(message), partitionForData(message),
                        keyForData(message), (StatPartDto) message.getPayload());*/
                //TODO А это - без него
                return super.send(message);
            }

            private String keyForData(Message<?> message) {
                return ((StatPartDto) message.getPayload()).getUri();
            }

            @Nonnull
            private String topicForData(Message<?> message) {
                //String topic = (String) message.getHeaders().get("kafka_topic");
                String topic = (String) message.getHeaders().get(KafkaHeaders.TOPIC);
                return Objects.requireNonNullElse(topic, "");
            }

            @Nonnull
            private Integer partitionForData(Message<?> message) {
                //Integer partition = (Integer) message.getHeaders().get("kafka_partition");
                Integer partition = (Integer) message.getHeaders().get(KafkaHeaders.PARTITION);
                return Objects.requireNonNullElse(partition, 0);
            }
        };
    }

        /*return new KafkaTemplate<>(replyingProducerFactory()) {
            @Override
            public CompletableFuture<SendResult<String, StatPartDto>> send(String topic,
                                                                      StatPartDto data) {
                return super.send(topic, 0, keyForData(data), data);
            }

            private String keyForData(StatPartDto statPartDto) {
                return statPartDto.getUri();
            }

        };*/
}
