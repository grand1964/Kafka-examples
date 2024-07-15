package ru.yandex.grand1964.kafka_demo.config;

import jakarta.annotation.Nonnull;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.adapter.ReplyHeadersConfigurer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.lang.NonNullApi;
import org.springframework.messaging.Message;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    @Autowired
    KafkaTemplate<String, StatPartDto> replyingKafkaTemplate;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;

    @Bean
    public ConsumerFactory<String, StatInDto> inConsumerFactory() {
        HashMap<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "ru.yandex.grand1964.kafka_demo.dto");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConsumerFactory<String, StatPartDto> outConsumerFactory() {
        HashMap<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "ru.yandex.grand1964.kafka_demo.dto");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StatInDto> inKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, StatInDto> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(inConsumerFactory());
        factory.setReplyTemplate(replyingKafkaTemplate);
        //factory.setReplyHeadersConfigurer((k,v) -> k.equals("key"));
        //factory.setReplyHeadersConfigurer((k,v) -> true);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StatPartDto> outKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, StatPartDto> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(outConsumerFactory());
        return factory;
    }

    //TODO Вернуть
    /*@Bean
    public Map<String, Object> replyingProducerConfigs() {
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
    public ProducerFactory<String, StatPartDto> replyingProducerFactory() {
        return new DefaultKafkaProducerFactory<>(replyingProducerConfigs());
    }

    @Bean
    public KafkaTemplate<String, StatPartDto> replyingKafkaTemplate() {
        return new KafkaTemplate<>(replyingProducerFactory()) {
            //@Override
            //public CompletableFuture<SendResult<String, StatPartDto>> send(String topic,
                                                                           StatPartDto data) {
            //    return super.send(topic, 0, keyForData(data), data);
            //}

            @Override
            @Nonnull
            public CompletableFuture<SendResult<String, StatPartDto>> send(@Nonnull Message<?> message) {
                return super.send(topicForData(message), partitionForData(message),
                        keyForData(message), (StatPartDto) message.getPayload());
            }

            private String keyForData(Message<?> message) {
                return ((StatPartDto) message.getPayload()).getUri();
            }

            @Nonnull
            private String topicForData(Message<?> message) {
                String topic = (String) message.getHeaders().get("kafka_topic");
                return Objects.requireNonNullElse(topic, "");
            }

            @Nonnull
            private Integer partitionForData(Message<?> message) {
                Integer partition = (Integer) message.getHeaders().get("kafka_partition");
                return Objects.requireNonNullElse(partition, 0);
            }
        };
    }*/
}
