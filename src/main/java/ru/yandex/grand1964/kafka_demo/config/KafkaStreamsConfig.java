package ru.yandex.grand1964.kafka_demo.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;
    @Value("${spring.application.name}")
    private String applicationId;
    @Value("${stat.double.disable}")
    private boolean doubleDisable;

    //TODO Вернуть !!!!!!!!!!!!!!!!!

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
        //props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        //TODO ??????????????????????????????????
        //props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, StatPartDto.class);
        //props.put("spring.json.use.type.headers", "false");
        //TODO ??????????????????????????????????
        props.put(JsonDeserializer.TYPE_MAPPINGS,
                "full:ru.yandex.grand1964.kafka_demo.dto.StatInDto," +
                        "part:ru.yandex.grand1964.kafka_demo.dto.StatPartDto");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "ru.yandex.grand1964.kafka_demo.dto");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        return new KafkaStreamsConfiguration(props);
    }

    /*@Bean
    public StreamsBuilderFactoryBeanCustomizer customizer() {
        return fb -> fb.setStateListener((newState, oldState) -> {
            System.out.println("State transition from " + oldState + " to " + newState);
        });
    }*/

    //TODO Версия с прямым подсчетом записей
    /*@Bean
    public KStream<String, Long> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, StatPartDto> inStream = kStreamBuilder.stream("app_name-ewm-main-service");
        KStream<String, Long> outStream = inStream
                .groupByKey()
                .count()
                .toStream();
        outStream.to("sink-topic", Produced.with(Serdes.String(), Serdes.Long()));
        outStream.print(Printed.toSysOut());
        return outStream;
    }*/

    //TODO ЭКСПЕРИМЕНТ БЕЗ ПРЕОБРАЗОВАНИЙ ТИПА ???????????????????
    /*@Bean
    public KStream<String, StatPartDto> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, StatPartDto> inStream = kStreamBuilder.stream("app_name-ewm-main-service");
        KStream<String, StatPartDto> outStream = inStream
                .mapValues(v -> {v.setIp("17"); return v;});
        outStream.to("sink-topic");
        outStream.print(Printed.toSysOut());
        return outStream;
    }*/

    //TODO Версия с подсчетом различных записей

    /*@Bean
    public DefaultJackson2JavaTypeMapper getTypeMapper() {
        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
        typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.TYPE_ID);
        typeMapper.addTrustedPackages("ru.yandex.grand1964.kafka_demo.dto");
        Map<String, Class<?>> mappings = new HashMap<>();
        mappings.put("full", ru.yandex.grand1964.kafka_demo.dto.StatInDto.class);
        mappings.put("part", ru.yandex.grand1964.kafka_demo.dto.StatPartDto.class);
        typeMapper.setIdClassMapping(mappings);
        return typeMapper;
    }

    @Bean
    public KStream<String, Integer> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, Integer> stream = kStreamBuilder.stream("app_name-ewm-main-service",
                        Consumed.with(Serdes.String(), new JsonSerde<>(StatPartDto.class)
                                .typeMapper(getTypeMapper())))
                .groupByKey()
                .aggregate(HashSet<StatPartDto>::new,
                        (k, v, r) -> {r.add(v); return r;})
                .mapValues(Set::size)
                .toStream();
        stream.to("sink-topic", Produced.with(Serdes.String(), Serdes.Integer()));
        stream.print(Printed.toSysOut());
        return stream;
    }*/

    @Bean
    public KStream<String, Integer> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, StatPartDto> inStream = kStreamBuilder.stream("app_name-ewm-main-service");
        KStream<String, Integer> outStream = inStream
                .groupByKey()
                .aggregate(HashSet<StatPartDto>::new,
                        (k, v, r) -> {r.add(v); return r;})
                //.toStream()
                .mapValues(Set::size)
                .toStream();
        outStream.to("sink-topic", Produced.with(Serdes.String(), Serdes.Integer()));
        outStream.print(Printed.toSysOut());
        return outStream;
    }

    //TODO Универсальная версия
    /*@Bean
    public KStream<String, StatPartDto> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, StatPartDto> inStream = kStreamBuilder.stream("app_name-ewm-main-service");
        //KStream<String, Integer> outStream = kStreamBuilder.stream("sink-topic");
        new KafkaStreamBrancher<String, StatPartDto>()
                .branch((k, v) -> doubleDisable,
                    ks -> ks
                        .groupByKey()
                        .aggregate(HashSet<StatPartDto>::new,
                            (k, v, r) -> {
                                r.add(v);
                                return r;
                            })
                        .mapValues(Set::size)
                        .toStream()
                        .to("sink-topic"))
                .defaultBranch(ks -> ks
                        .groupByKey()
                        .count()
                        .toStream()
                        .to("sink-topic"))
                .onTopOf(inStream);
        return inStream;
    }*/

    /*@Bean
    public KStream<Integer, String> kStream(StreamsBuilder kStreamBuilder) {
        KStream<Integer, String> stream = kStreamBuilder.stream("streamingTopic1");
        stream
                .mapValues((ValueMapper<String, String>) String::toUpperCase)
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMillis(1000)))
                .reduce((String value1, String value2) -> value1 + value2,
                        Named.as("windowStore"))
                .toStream()
                .map((windowedId, value) -> new KeyValue<>(windowedId.key(), value))
                .filter((i, s) -> s.length() > 40)
                .to("streamingTopic2");

        stream.print(Printed.toSysOut());

        return stream;
    }*/
}
