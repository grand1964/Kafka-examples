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
//@SpringJUnitConfig
@DirtiesContext
@TestPropertySource(locations = "classpath:test.properties")
@EmbeddedKafka(partitions = 1, topics = {"${input.topic.name}", "${sink.topic.name}"})
        /*brokerProperties = {"auto.create.topics.enable=${auto.create.topics.enable}"},
        brokerPropertiesLocation = "classpath:test.properties")*/
public class WithDuplePunctuationTests {
    /*@Value("${input.topic.name}")
    private String INPUT_TOPIC_NAME;
    @Value("${sink.topic.name}")
    private String SINK_TOPIC_NAME;
    @Value("${stat.punctuation.step}")
    private int punctuationStep;*/
    private static final String INPUT_TOPIC_NAME = "ewm-main-service";
    private static final String SINK_TOPIC_NAME = "sink";
    //TODO ИСПРАВИТЬ!!!!!!
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

    //////////////////////////// Тест пунктуации /////////////////////////////

    @Test
    @DisplayName("УБРАТЬ!!!!")
    public void testPunctuation() {
        //выравниваем текущее время кратно шагу пунктуации
        //1730719680000 - базовое значение
        //LocalDateTime timestamp = TestUtils.toDateTime(1730719680000L).toLocalDateTime(); //для 5 и 6
        //LocalDateTime timestamp = TestUtils.toDateTime(1730719690000L).toLocalDateTime(); //для 7
        //LocalDateTime timestamp = TestUtils.toDateTime(1730719650000L).toLocalDateTime(); //для 11
        LocalDateTime timestamp = TestUtils.toDateTime(0L).toLocalDateTime(); //??????????????

        //задаем число объектов
        int eventCount = 12;
        int[] times = {0,11,22};
        //int[] times = {0,14,21,22,23,24,25,34,35,42,49,56,111}; //,63,70,77};
        //int[] times = {0,1,2,3,4,5,6,7,8,9,10, 14,17,21, 33,44,55,66,77};
        //int[] times = {0,1,2,3,4,5,6,7,8,9,10, 17,21, 33,35,42, 44,47,49,51, 55,66,77};
        //int[] times = {0,1,2,3,4,5,6,7,8,9,10,  11,12,  22,23,24,  33,34,35,36,37,   44,45,46,47,48,49,  55,  66,  77};
        //int[] times = {0,1,2,3,4,5,6,7,8,9,10,  11,12,13,  22,23,24,25,26,27,  33,34,   44,45,46,47,48,  55,  66,  77};
        //int[] times = {0,1,2,3,4,5,6,7,8,9,10,  11,12,13,14,  22,  33,34,35,   44,45,  55,  66,  77};
        //int[] times = {0,1,2,3,4,5,6,7,8,9,10,  11,12,13,14,  22,  33,   44,  55,56,57,  66,  77};
        //int[] times = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,16,16, 17,18,19,  34,  51,  68,  85,86,87,  102, 119};
        //int[] times = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,16,16,16, 17,18,19};
        //int[] times = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,16,16,16,16, 17,18,19, 68, 102};
        //int[] times = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,16,16,16,16, 17,18,19, 34, 51, 68, 85, 102};
        //int[] times = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16, 17,  34,  51,  68,  85,  102, 119};
        //int eventTimeStep = 7;

        //TODO ВЕРНУТЬ!!!!!!!!!!!!
        //создаем потребителя
        Consumer<String, Object> consumer = sinkConsumerFactory.createConsumer();
        //подписываемся на тему
        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, SINK_TOPIC_NAME);
        //переменная для результата
        ConsumerRecords<String, Object> replies;

        //TODO ВЕРНУТЬ!!!
        //генерируем объекты
        //List<Message<StatPartDto>> messages = TestUtils.generateMockedTimestamps(INPUT_TOPIC_NAME, times);


        /*List<Message<StatPartDto>> messages = new ArrayList<>();
        for (int i = 0; i < eventCount / 2; i++) {
            StatInDto dto = TestUtils.generateInputObject(
                    INPUT_TOPIC_NAME, 1, i, timestamp.plusSeconds(i * eventTimeStep));
            messages.add(TestUtils.fromStatInDto(dto));
        }
        for (int i = eventCount / 2; i < eventCount; i++) {
            StatInDto dto = TestUtils.generateInputObject(
                    INPUT_TOPIC_NAME, 2, i, timestamp.plusSeconds(i * eventTimeStep));
            messages.add(TestUtils.fromStatInDto(dto));
        }*/

        //фиктивный запрос для активизации потока
        //KafkaTestUtils.getRecords(consumer, Duration.between(timestamp, timestamp.plusSeconds(1)));
        //KafkaTestUtils.getRecords(consumer, Duration.ofMillis(20000));
        //KafkaTestUtils.getRecords(consumer);

        pause(3000);

        List<Message<StatPartDto>> messages = TestUtils.generateMockedTimestamps(INPUT_TOPIC_NAME, times);
        for (int i = 0; i < times.length; i++) {
            kafkaTemplate.send(messages.get(i));
            kafkaTemplate.flush();
            pause(100);
        }
        System.out.println("XPEH BAM");

        //TODO ЭКСПЕРИМЕНТ
        /*for (int i = 0; i < times.length; i++) {
            kafkaTemplate.send(messages.get(i));
            kafkaTemplate.flush();
        }*/

        /*int blockSize = 15;
        int blockCount = times.length / blockSize;
        int blockRes = times.length % blockSize;
        for (int i = 0; i < blockCount; i++) {
            int offset = i * blockSize;
            for (int j = 0; j < blockSize; j++) {
                kafkaTemplate.send(messages.get(offset + j));
                kafkaTemplate.flush();
            }
            replies = KafkaTestUtils.getRecords(consumer);
            for (ConsumerRecord<String, Object> reply : replies) {
                System.out.println("[Synthesized data]: " + reply.value());
                System.out.println("Системное время: " + LocalDateTime.now());
            }
        }
        if (blockRes > 0) {
            int offset = blockSize * blockCount;
            for (int j = 0; j < blockRes; j++) {
                kafkaTemplate.send(messages.get(offset + j));
                kafkaTemplate.flush();
            }
            replies = KafkaTestUtils.getRecords(consumer);
            for (ConsumerRecord<String, Object> reply : replies) {
                System.out.println("[Synthesized data]: " + reply.value());
                System.out.println("Системное время: " + LocalDateTime.now());
            }
        }*/

        /*kafkaTemplate.executeInTransaction(t -> {
            for (int i = 0; i < times.length; i++) {
                t.send(messages.get(i));
                t.flush();
            }
            return true;
        });*/


        /*//посылаем первую порцию сообщений
        for (int i = 0; i < eventCount / 2; i++) {
            kafkaTemplate.send(messages.get(i));
            kafkaTemplate.flush();
        }

        KafkaTestUtils.getRecords(consumer, Duration.ofMillis(20000));

        //посылаем вторую порцию сообщений
        for (int i = eventCount / 2; i < eventCount; i++) {
            kafkaTemplate.send(messages.get(i));
            kafkaTemplate.flush();
        }*/

        //TODO ВЕРНУТЬ!!!!!!!!!!!!
        //читаем результат, вызывая пунктуацию
        replies = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(10000));
        //replies = KafkaTestUtils.getRecords(consumer);
        List<ConsumerRecord<String, Object>> records = replies.records(new TopicPartition(SINK_TOPIC_NAME, 0));

        //читаем синтезированные записи
        for (ConsumerRecord<String, Object> record : records) {
            System.out.println("[Synthesized data]: " + record.value());
            System.out.println("Системное время: " + LocalDateTime.now());
        }

        //запись должна быть одна
        //assertEquals(replies.count(), 2);
        /*assertEquals(replies.count(), 1);
        StatOutDto result = (StatOutDto) records.get(0).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(result.getUri(), records.get(0).key());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 3);*/
    }

    //TODO ??????????????????????????????
    /*@Transactional
    private void sendAll(int[] times) {
        List<Message<StatPartDto>> messages = TestUtils.generateMockedTimestamps(INPUT_TOPIC_NAME, times);
        for (int i = 0; i < times.length; i++) {
            kafkaTemplate.send(messages.get(i));
            kafkaTemplate.flush();
        }
    }*/

    //////////////////////////////////////////////////////////////////////////
    ///////////////////////// Тесты с дублированием //////////////////////////
    //////////////////////////////////////////////////////////////////////////

    @Test
    @DisplayName("Тест с одной записью")
    public void testOneRecord() {
        LocalDateTime timestamp = TestUtils.toDateTime(0).toLocalDateTime();
        int[] times = {0,3,5,7,9,10, 23,27,32};

        //создаем потребителя
        Consumer<String, Object> consumer = sinkConsumerFactory.createConsumer();
        //подписываемся на тему
        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, SINK_TOPIC_NAME);

        //пауза для завершения инициализации
        pause(3000);

        //посылаем записи
        List<Message<StatPartDto>> messages = TestUtils.generateMockedTimestamps(INPUT_TOPIC_NAME, times);
        for (int i = 0; i < times.length; i++) {
            kafkaTemplate.send(messages.get(i));
            kafkaTemplate.flush();
            pause(100);
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
        assertEquals(replies.count(), 2);

        //анализируем первую запись
        StatOutDto result = (StatOutDto) records.get(0).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(0).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 1);
        //время соответствует времени потока
        assertEquals(records.get(0).timestamp(), 0);

        //анализируем последнюю запись
        result = (StatOutDto) records.get(1).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(1).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 7);
        //время соответствует времени потока
        assertEquals(records.get(1).timestamp(), 23000);
    }

    @Test
    @DisplayName("Тест с тремя ключами")
    public void testSeveralKeys() {
        LocalDateTime timestamp = TestUtils.toDateTime(0).toLocalDateTime();
        int[] times = {0,7, 13,17,19,21, 27,32, 35,39};
        int[] uris = {1,1,1,1, 2,2,2, 3,3,3};

        //создаем потребителя
        Consumer<String, Object> consumer = sinkConsumerFactory.createConsumer();
        //подписываемся на тему
        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, SINK_TOPIC_NAME);

        //пауза для завершения инициализации
        pause(3000);

        //посылаем записи
        List<Message<StatPartDto>> messages = TestUtils.generateMockedTimestamps(INPUT_TOPIC_NAME, times, uris);
        for (int i = 0; i < times.length; i++) {
            kafkaTemplate.send(messages.get(i));
            kafkaTemplate.flush();
            pause(100);
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
        assertEquals(replies.count(), 7);

        //анализируем вторую запись
        StatOutDto result = (StatOutDto) records.get(1).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(1).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 3);
        //время соответствует времени потока
        assertEquals(records.get(1).timestamp(), 13000);

        //анализируем третью запись
        result = (StatOutDto) records.get(2).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(2).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 4);
        //время соответствует времени потока
        assertEquals(records.get(2).timestamp(), 27000);

        //анализируем четвертую запись
        result = (StatOutDto) records.get(3).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(3).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 3);
        //время соответствует времени потока
        assertEquals(records.get(3).timestamp(), 27000);

        //анализируем седьмую запись
        result = (StatOutDto) records.get(6).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(6).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 2);
        //время соответствует времени потока
        assertEquals(records.get(6).timestamp(), 35000);
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
        List<Message<StatPartDto>> messages = TestUtils.generateMockedTimestamps(INPUT_TOPIC_NAME, times);
        for (int i = 0; i < times.length; i++) {
            kafkaTemplate.send(messages.get(i));
            kafkaTemplate.flush();
            pause(100);
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
    @DisplayName("Оконный тест с двумя ключами")
    public void testTwoKeysWithShortWindow() {
        LocalDateTime timestamp = TestUtils.toDateTime(0).toLocalDateTime();
        int[] times = {0,5,7, 13,17,21, 46,49,52, 66, 110};
        int[] uris = {1,1,1,1, 2,2,2,2,2,2,2};

        //создаем потребителя
        Consumer<String, Object> consumer = sinkConsumerFactory.createConsumer();
        //подписываемся на тему
        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, SINK_TOPIC_NAME);

        //пауза для завершения инициализации
        pause(3000);

        //посылаем записи
        List<Message<StatPartDto>> messages = TestUtils.generateMockedTimestamps(INPUT_TOPIC_NAME, times, uris);
        for (int i = 0; i < times.length; i++) {
            kafkaTemplate.send(messages.get(i));
            kafkaTemplate.flush();
            pause(100);
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
        assertEquals(replies.count(), 8);

        //анализируем вторую запись
        StatOutDto result = (StatOutDto) records.get(1).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(1).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 4);
        //время соответствует времени потока
        assertEquals(records.get(1).timestamp(), 13000);

        //анализируем третью запись
        result = (StatOutDto) records.get(2).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(2).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 4);
        //время соответствует времени потока
        assertEquals(records.get(2).timestamp(), 46000);

        //анализируем четвертую запись
        result = (StatOutDto) records.get(3).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(3).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 3);
        //время соответствует времени потока
        assertEquals(records.get(3).timestamp(), 46000);

        //анализируем шестую запись
        result = (StatOutDto) records.get(5).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(5).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 4);
        //время соответствует времени потока
        assertEquals(records.get(5).timestamp(), 66000);

        //анализируем восьмую запись
        result = (StatOutDto) records.get(7).value();
        //ключ результата должен совпадать с URI вложения
        assertEquals(records.get(7).key(), result.getUri());
        //поле hits должно соответствовать количеству записей
        assertEquals(result.getHits(), 1);
        //время соответствует времени потока
        assertEquals(records.get(7).timestamp(), 110000);
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
            props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
            //Частота сброса кэша на диск
            props.put("commit.interval.ms", 10000);
            return new KafkaStreamsConfiguration(props);
        }

        @Bean
        public StatBuilder statBuilder() {
            return new StatBuilder(INPUT_TOPIC_NAME, SINK_TOPIC_NAME, false, windowSize, punctuationStep);
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
