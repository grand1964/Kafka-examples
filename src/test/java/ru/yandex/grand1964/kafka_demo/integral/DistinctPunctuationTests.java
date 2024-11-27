package ru.yandex.grand1964.kafka_demo.integral;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import ru.yandex.grand1964.kafka_demo.TestUtils;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.dto.StatOutDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;
import ru.yandex.grand1964.kafka_demo.punctuator.StatTimestampExtractor;
import ru.yandex.grand1964.kafka_demo.topology.StatBuilder;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@DirtiesContext
@TestPropertySource(locations = "classpath:test.properties")
@EmbeddedKafka(partitions = 1, topics = {"${input.topic.name}", "${sink.topic.name}"})
        /*brokerProperties = {"auto.create.topics.enable=${auto.create.topics.enable}"},
        brokerPropertiesLocation = "classpath:test.properties")*/
public class DistinctPunctuationTests {
    private static final String INPUT_TOPIC_NAME = "ewm-main-service";
    private static final String SINK_TOPIC_NAME = "sink";
    private static final Duration windowSize = Duration.ofMillis(40000);
    private static final Duration punctuationStep = Duration.ofMillis(11000);

    @Value("${time.format.pattern}")
    private String timePattern;
    @Autowired
    EmbeddedKafkaBroker embeddedKafka;
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired
    @Qualifier("inputConsumerFactory")
    ConsumerFactory<String, Object> inputConsumerFactory;
    @Autowired
    @Qualifier("sinkConsumerFactory")
    ConsumerFactory<String, Object> sinkConsumerFactory;
    @Autowired
    @Qualifier("&statStreamsBuilderFactory")
    private StreamsBuilderFactoryBean factory;

    @BeforeEach
    public void setUp() {
        factory.start();
    }

    @AfterEach
    public void tearDown() {
        factory.stop();
    }

    ////////////////////////// Передача управления ///////////////////////////

    private void pause(int msDuration) {
        try {
            Thread.sleep(msDuration);
        } catch (InterruptedException e) {
            throw new RuntimeException("");
        }
    }


    //TODO УБРАТЬ!!!!!!!


    @Test
    public void testMissing() {
        //ЭТОТ ТЕСТ ТРЕБУЕТ ПОДКЛЮЧИТЬ testMissingBuilder В StatBuilder.configureBuilder

        ConsumerRecords<String, Object> replies;
        //фиксируем время
        LocalDateTime timestamp = TestUtils.toDateTime(0).toLocalDateTime();

        //TODO ПЕРЕМЕШАТЬ ОТСЕВ В РАЗНЫХ ТОЧКАХ
        int[] times = {0, 23, 33, 44};
        //int[] times = {0,7, 23,25,27, 33, 55, 68, 79};
        int[] uris = {1,2,1,1};
        //int[] uris = {1,1, 2,1,2, 1, 1, 2, 1};
        List<Message<StatPartDto>> messages =
                TestUtils.generateMockedTimestamps(INPUT_TOPIC_NAME, times, uris, i -> i);
        /*StatInDto dto0 = TestUtils.generateMockedInputObject(INPUT_TOPIC_NAME, timestamp.plusSeconds(11), 1);
        Message<StatPartDto> message0 = TestUtils.fromStatInDto(dto0);
        messages.add(1, message0);*/
        /*StatInDto dto1 = TestUtils.generateMockedInputObject(INPUT_TOPIC_NAME, timestamp.plusSeconds(44), 2);
        Message<StatPartDto> message1 = TestUtils.fromStatInDto(dto1);
        messages.add(7, message1);*/

        //int[] times = {0, 11, 23, 33};
        //int[] uris = {1, 2, 1, 1};
        //int[] uris = {1, 2, 2, 2, 2, 2};
        //int[] uris = {1, 2, 1, 2, 1, 1};

        //int[] times = {0};
        //int[] times = {0,7,11, 23,27, 33};
        //int[] times = {11, 23, 33};
        //int[] times = {0, 22, 33, 44};
        //int[] times = {22,23,27, 33,42, 44};

        //создаем потребителя
        /*Consumer<String, Object> consumer = sinkConsumerFactory.createConsumer();
        //подписываемся на тему
        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, SINK_TOPIC_NAME);*/

        //пауза для завершения инициализации
        pause(3000);

        //посылаем записи
        /*List<Message<StatPartDto>> messages =
                //TestUtils.generateMockedTimestamps(INPUT_TOPIC_NAME, times, i -> 1, i -> 1);
                TestUtils.generateMockedTimestamps(INPUT_TOPIC_NAME, times, uris, i -> i);
        StatInDto dto0 = TestUtils.generateMockedInputObject(INPUT_TOPIC_NAME, timestamp, 1);
        Message<StatPartDto> message0 = TestUtils.fromStatInDto(dto0);
        messages.add(1,message0);
        StatInDto dto1 = TestUtils.generateMockedInputObject(INPUT_TOPIC_NAME, timestamp.plusSeconds(11), 2);
        Message<StatPartDto> message1 = TestUtils.fromStatInDto(dto1);
        messages.add(1,message1);*/

        for (int i = 0; i < messages.size(); i++) {
            kafkaTemplate.send(messages.get(i));
            kafkaTemplate.flush();
            pause(500);
        }

        /*replies = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(10000));
        for (ConsumerRecord<String, Object> reply : replies) {
            System.out.println(reply.value());
        }*/
        //assertEquals(replies.count(), 1);
    }

    //////////////////////////////////////////////////////////////////////////
    ///////////////////////// Тесты без дублирования /////////////////////////
    //////////////////////////////////////////////////////////////////////////

    @Test
    @DisplayName("Тест с одной записью")
    public void testOneRecord() {
        LocalDateTime timestamp = TestUtils.toDateTime(0).toLocalDateTime();
        //int[] times = {0,3,7,9,10, 23,27,32};
        int[] times = {0,11, 23};

        //создаем потребителя
        Consumer<String, Object> consumer = sinkConsumerFactory.createConsumer();
        //подписываемся на тему
        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, SINK_TOPIC_NAME);

        //пауза для завершения инициализации
        pause(3000);

        //посылаем записи
        List<Message<StatPartDto>> messages =
                TestUtils.generateMockedTimestamps(INPUT_TOPIC_NAME, times, i -> 1, i -> 1);
        for (int i = 0; i < times.length; i++) {
            kafkaTemplate.send(messages.get(i));
            kafkaTemplate.flush();
            pause(500);
        }
        //читаем результат
        ConsumerRecords<String, Object> replies = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(10000));
        List<ConsumerRecord<String, Object>> records = replies.records(new TopicPartition(SINK_TOPIC_NAME, 0));

        //читаем синтезированные записи
        for (ConsumerRecord<String, Object> record : records) {
            System.out.println("[Synthesized data]: " + record.value());
            System.out.println("Системное время: " + LocalDateTime.now());
        }

        //число записей соответствует числу пунктуаций
        assertEquals(replies.count(), 1);

        //анализируем первую запись
        StatOutDto result = (StatOutDto) records.get(0).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(0).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 1);
        //время соответствует времени потока
        assertEquals(records.get(0).timestamp(), 0);
}

    @Test
    @DisplayName("Тест с двумя записями")
    public void testTwoRecords() {
        LocalDateTime timestamp = TestUtils.toDateTime(0).toLocalDateTime();
        int[] times = {0,7,10, 11, 23};
        int[] uris = {1,2,2, 1,2};
        int[] ips = {1,1,1, 2,2};

        //создаем потребителя
        Consumer<String, Object> consumer = sinkConsumerFactory.createConsumer();
        //подписываемся на тему
        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, SINK_TOPIC_NAME);

        //пауза для завершения инициализации
        pause(3000);

        //посылаем записи
        List<Message<StatPartDto>> messages =
                TestUtils.generateMockedTimestamps(INPUT_TOPIC_NAME, times, uris, ips);
        for (int i = 0; i < times.length; i++) {
            kafkaTemplate.send(messages.get(i));
            kafkaTemplate.flush();
            pause(500);
        }
        //читаем результат
        ConsumerRecords<String, Object> replies = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(10000));
        List<ConsumerRecord<String, Object>> records = replies.records(new TopicPartition(SINK_TOPIC_NAME, 0));

        //читаем синтезированные записи
        for (ConsumerRecord<String, Object> record : records) {
            System.out.println("[Synthesized data]: " + record.value());
            System.out.println("Системное время: " + LocalDateTime.now());
        }

        //число записей: 1 с первой пунктуации и по 2 со второй и третьей
        assertEquals(replies.count(), 5);

        //анализируем первую запись
        StatOutDto result = (StatOutDto) records.get(0).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(0).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 1);
        //время соответствует времени потока
        assertEquals(records.get(0).timestamp(), 0);

        //анализируем вторую запись
        result = (StatOutDto) records.get(1).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(1).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 2);
        //время соответствует времени потока
        assertEquals(records.get(1).timestamp(), 11000);

        //анализируем третью запись
        result = (StatOutDto) records.get(2).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(2).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 1);
        //время соответствует времени потока
        assertEquals(records.get(2).timestamp(), 11000);

        //анализируем четвертую запись
        result = (StatOutDto) records.get(3).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(3).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 2);
        //время соответствует времени потока
        assertEquals(records.get(3).timestamp(), 23000);

        //анализируем пятую запись
        result = (StatOutDto) records.get(4).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(4).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 2);
        //время соответствует времени потока
        assertEquals(records.get(4).timestamp(), 23000);
    }

    @Test
    @DisplayName("Оконный тест с одной записью")
    public void testOneRecordWithShortWindow() {
        LocalDateTime timestamp = TestUtils.toDateTime(0).toLocalDateTime();
        int[] times = {0,6,7,10, 23,27,32, 46,49, 66, 110};

        //создаем потребителя
        Consumer<String, Object> consumer = sinkConsumerFactory.createConsumer();
        //подписываемся на тему
        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, SINK_TOPIC_NAME);

        //пауза для завершения инициализации
        pause(3000);

        //посылаем записи
        List<Message<StatPartDto>> messages = TestUtils.generateMockedTimestamps(
                INPUT_TOPIC_NAME, times, i -> 1, i -> i);
        for (int i = 0; i < times.length; i++) {
            kafkaTemplate.send(messages.get(i));
            kafkaTemplate.flush();
            pause(500);
        }
        //читаем результат
        ConsumerRecords<String, Object> replies = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(10000));
        //replies = KafkaTestUtils.getRecords(consumer);
        List<ConsumerRecord<String, Object>> records = replies.records(new TopicPartition(SINK_TOPIC_NAME, 0));

        //читаем синтезированные записи
        for (ConsumerRecord<String, Object> record : records) {
            System.out.println("[Synthesized data]: " + record.value());
            System.out.println("Системное время: " + LocalDateTime.now());
        }

        //число записей соответствует числу пунктуаций
        assertEquals(replies.count(), 5);

        //анализируем первую запись
        StatOutDto result = (StatOutDto) records.get(0).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(0).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 1);
        //время соответствует времени потока
        assertEquals(records.get(0).timestamp(), 0);

        //анализируем вторую запись
        result = (StatOutDto) records.get(1).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(1).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 5);
        //время соответствует времени потока
        assertEquals(records.get(1).timestamp(), 23000);

        //анализируем третью запись
        result = (StatOutDto) records.get(2).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(2).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 7);
        //время соответствует времени потока
        assertEquals(records.get(2).timestamp(), 46000);

        //анализируем четвертую запись
        result = (StatOutDto) records.get(3).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(3).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 5);
        //время соответствует времени потока
        assertEquals(records.get(3).timestamp(), 66000);

        //анализируем пятую запись
        result = (StatOutDto) records.get(4).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(4).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 1);
        //время соответствует времени потока
        assertEquals(records.get(4).timestamp(), 110000);
    }

    @Test
    @DisplayName("Тест с дублями и коротким окном")
    public void testWithDupleAndShortWindow() {
        LocalDateTime timestamp = TestUtils.toDateTime(0).toLocalDateTime();
        int[] times = {0,6,7,10, 23,27,32, 46,49, 66};
        int[] uris = {1,3,1,1, 2,2,2, 3,3, 2};
        int[] ips = {1,1,1,1, 2,3,2, 2,3, 1};

        //создаем потребителя
        Consumer<String, Object> consumer = sinkConsumerFactory.createConsumer();
        //подписываемся на тему
        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, SINK_TOPIC_NAME);

        //пауза для завершения инициализации
        pause(3000);

        //посылаем записи
        List<Message<StatPartDto>> messages =
                TestUtils.generateMockedTimestamps(INPUT_TOPIC_NAME, times, uris, ips);
        for (int i = 0; i < times.length; i++) {
            kafkaTemplate.send(messages.get(i));
            kafkaTemplate.flush();
            pause(500);
        }
        //читаем результат
        ConsumerRecords<String, Object> replies = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(10000));
        List<ConsumerRecord<String, Object>> records = replies.records(new TopicPartition(SINK_TOPIC_NAME, 0));

        //читаем синтезированные записи
        for (ConsumerRecord<String, Object> record : records) {
            System.out.println("[Synthesized data]: " + record.value());
            System.out.println("Системное время: " + LocalDateTime.now());
        }

        //число записей: 1 с первой пунктуации и по 3 со второй, третьей и четвертой
        assertEquals(replies.count(), 10);

        //анализируем первую запись
        StatOutDto result = (StatOutDto) records.get(0).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(0).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 1);
        //время соответствует времени потока
        assertEquals(records.get(0).timestamp(), 0);

        //анализируем вторую запись
        result = (StatOutDto) records.get(1).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(1).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 1);
        //время соответствует времени потока
        assertEquals(records.get(1).timestamp(), 23000);

        //анализируем третью запись
        result = (StatOutDto) records.get(2).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(2).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 1);
        //время соответствует времени потока
        assertEquals(records.get(2).timestamp(), 23000);

        //анализируем четвертую запись
        result = (StatOutDto) records.get(3).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(3).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 1);
        //время соответствует времени потока
        assertEquals(records.get(3).timestamp(), 23000);

        //анализируем пятую запись
        result = (StatOutDto) records.get(4).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(4).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 1);
        //время соответствует времени потока
        assertEquals(records.get(4).timestamp(), 46000);

        //анализируем шестую запись
        result = (StatOutDto) records.get(5).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(5).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 2);
        //время соответствует времени потока
        assertEquals(records.get(5).timestamp(), 46000);

        //анализируем седьмую запись
        result = (StatOutDto) records.get(6).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(6).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 2);
        //время соответствует времени потока
        assertEquals(records.get(6).timestamp(), 46000);

        //анализируем восьмую запись
        result = (StatOutDto) records.get(7).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(7).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 1);
        //время соответствует времени потока
        assertEquals(records.get(7).timestamp(), 66000);

        //анализируем девятую запись
        result = (StatOutDto) records.get(8).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(8).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 2);
        //время соответствует времени потока
        assertEquals(records.get(8).timestamp(), 66000);

        //анализируем десятую запись
        result = (StatOutDto) records.get(9).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(9).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 2);
        //время соответствует времени потока
        assertEquals(records.get(9).timestamp(), 66000);
    }

    //////////////////////////////////////////////////////////////////////////
    ////////////////////////////// Конфигурации //////////////////////////////
    //////////////////////////////////////////////////////////////////////////

    /////////////////////////////// Streams //////////////////////////////////

    @Configuration
    public static class StreamsConfiguration {
        /*@Value("${spring.kafka.bootstrap-servers}")
        private String kafkaServer;*/
        @Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
        private String kafkaServer;
        @Value("${spring.application.name}")
        private String applicationId;

        //Бин-фабрика KafkaBuilder
        @Bean
        public StreamsBuilderFactoryBean statStreamsBuilderFactory(KafkaStreamsConfiguration streamsConfig) {
            StreamsBuilderFactoryBean factoryBean = new StreamsBuilderFactoryBean(streamsConfig);
            factoryBean.setAutoStartup(false); //автоматически не стартуем
            StatBuilder customizer = statBuilder();
            factoryBean.setInfrastructureCustomizer(customizer);
            return factoryBean;
        }

        //конфигурация фабрики KafkaBuilder
        @Bean
        public KafkaStreamsConfiguration streamsConfig() {
            Map<String, Object> props = new HashMap<>();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
            props.put(JsonDeserializer.TYPE_MAPPINGS,
                    "full:ru.yandex.grand1964.kafka_demo.dto.StatInDto," +
                            "part:ru.yandex.grand1964.kafka_demo.dto.StatPartDto," +
                            "out:ru.yandex.grand1964.kafka_demo.dto.StatOutDto");
            props.put(JsonDeserializer.TRUSTED_PACKAGES, "ru.yandex.grand1964.kafka_demo.dto");
            //props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, FailOnInvalidTimestamp.class.getName());
            props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, StatTimestampExtractor.class.getName());
            props.put(StreamsConfig.STATE_DIR_CONFIG, "D:\\Kafka\\test\\kafka-streams");
            //кэш таблицы отключаем, поскольку реализуем свой
            props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
            //частота сброса кэша на диск
            props.put("commit.interval.ms", 10000);
            return new KafkaStreamsConfiguration(props);
        }

        @Bean
        public StatBuilder statBuilder() {
            return new StatBuilder(INPUT_TOPIC_NAME, SINK_TOPIC_NAME, true, windowSize, punctuationStep);
        }

    }

    /////////////////////////////// Producer /////////////////////////////////

    @Configuration
    public static class KafkaProducerConfig {
        /*@Value("${spring.kafka.bootstrap-servers}")
        private String kafkaServer;*/
        @Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
        private String kafkaServer;

        //Фабрика продюсера с отображением типов
        @Bean
        public ProducerFactory<String, Object> multiProducerFactory() {
            //параметры продюсера
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            //props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "1");
            //отображение типов
            props.put(JsonSerializer.TYPE_MAPPINGS,
                    "full:ru.yandex.grand1964.kafka_demo.dto.StatInDto," +
                            "part:ru.yandex.grand1964.kafka_demo.dto.StatPartDto," +
                            "out:ru.yandex.grand1964.kafka_demo.dto.StatOutDto");
            return new DefaultKafkaProducerFactory<>(props);
        }

        //Template для работы с разными типами
        @Bean
        public KafkaTemplate<String, Object> multiKafkaTemplate() {
            return new KafkaTemplate<>(multiProducerFactory());
        }
    }

    /////////////////////////////// Consumer /////////////////////////////////

    @Configuration
    public static class KafkaConsumerConfig {
        @Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
        private String kafkaServer;
        @Value("${consumer.input.group-id}")
        private String inputGroupId;
        @Value("${consumer.sink.group-id}")
        private String sinkGroupId;

        @Bean
        public ConsumerFactory<String, Object> inputConsumerFactory() {
            return new DefaultKafkaConsumerFactory<>(consumerProps(inputGroupId));
        }

        @Bean
        public ConsumerFactory<String, Object> sinkConsumerFactory() {
            return new DefaultKafkaConsumerFactory<>(consumerProps(sinkGroupId));
        }

        private Map<String, Object> consumerProps(String groupId) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
            props.put(JsonDeserializer.TYPE_MAPPINGS,
                    "full:ru.yandex.grand1964.kafka_demo.dto.StatInDto," +
                            "part:ru.yandex.grand1964.kafka_demo.dto.StatPartDto," +
                            "out:ru.yandex.grand1964.kafka_demo.dto.StatOutDto");
            props.put(JsonDeserializer.TRUSTED_PACKAGES, "ru.yandex.grand1964.kafka_demo.dto");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            //props.put(ConsumerConfig.GROUP_ID_CONFIG, sinkGroupId);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);
            //TODO УБРАТЬ!!!!!
            //props.put("fetch.max.wait.ms", 10000);
            //props.put("default.api.timeout.ms", 10000);
            return props;
        }

        /*@Bean
        public ConcurrentKafkaListenerContainerFactory<String, Object> multiKafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(multiConsumerFactory());
            return factory;
        }*/
    }
}
