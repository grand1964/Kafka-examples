package ru.yandex.grand1964.kafka_demo.topology;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import ru.yandex.grand1964.kafka_demo.TestUtils;
import ru.yandex.grand1964.kafka_demo.dto.StatOutDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class WithDuplePunctuationTests {
    private static TopologyTestDriver topologyTestDriver;
    private static final String IN_TOPIC = "common";
    private static final String OUT_TOPIC = "sink";
    private static final String STORE_NAME_SLIDING = "slidingStore";
    private static Instant startTime;
    private static long winAdvanceSize;
    private static long clockAdvanceSize;
    private static Duration clockAdvance;
    private static TestInputTopic<String, StatPartDto> inputTopic;
    private static TestOutputTopic<String, StatOutDto> outputTopic;

    /////////////////////////// Общая конфигурация ///////////////////////////

    @BeforeAll
    public static void setUp() {
        startTime = Instant.now();
        Duration winSize = Duration.ofMillis(3000); //ширина окна
        winAdvanceSize = 100; //шаг по записям
        Duration winAdvance = Duration.ofMillis(winAdvanceSize);
        clockAdvanceSize = 6000;
        clockAdvance = Duration.ofMillis(clockAdvanceSize);
        DefaultJackson2JavaTypeMapper typeMapper = TestUtils.getTypeMapper();
        Topology topology =
                StatTopology.buildCountDupleWithPunctuationTopology(winSize, clockAdvance);
        topologyTestDriver = new TopologyTestDriver(topology,
                TestUtils.getProperties(), startTime);
        //создаем темы
        inputTopic = topologyTestDriver.createInputTopic(IN_TOPIC,
                new StringSerializer(),
                new JsonSerializer<StatPartDto>().typeMapper(typeMapper),
                startTime, winAdvance);
        outputTopic = topologyTestDriver.createOutputTopic(OUT_TOPIC,
                new StringDeserializer(),
                new JsonDeserializer<>(StatOutDto.class).typeMapper(typeMapper));
    }

    @AfterAll
    public static void tearDown() {
        topologyTestDriver.close();
    }

    //////////////////////////////////////////////////////////////////////////
    //////////////////// Тесты пунктуации без повторений /////////////////////
    //////////////////////////////////////////////////////////////////////////

    @Test
    @DisplayName("Testing distinct punctuation with single key")
    public void testDistinctSingleKeyPunctuation() {
        //пишем на вход две записи с одним ключом
        List<TestRecord<String, StatPartDto>> records =
                TestUtils.generateDistinctRecordsWithoutTimestamp(1,2);
        inputTopic.pipeRecordList(records);
        //читаем выходные записи
        List<TestRecord<String, StatOutDto>> outRecords = outputTopic.readRecordsToList();
        //их не должно быть (пунктуации еще не было)
        assertEquals(outRecords.size(), 0);

        //сдвигаем время
        topologyTestDriver.advanceWallClockTime(clockAdvance);
        //пишем на вход запись с другим ключом, после пунктуации
        inputTopic.advanceTime(Duration.ofMillis(clockAdvanceSize - 2 * winAdvanceSize));
        inputTopic.pipeInput(TestUtils.generateRecordWithoutTimestamp(2, 1));
        //читаем выходные записи и печатаем их
        outRecords = outputTopic.readRecordsToList();
        TestUtils.printRecords(outRecords);
        //пунктуация срабатывает раньше обработки новой записи ???
        assertEquals(outRecords.size(), 1);
        assertEquals(outRecords.get(0).key(), TestUtils.genKey(1));
        assertEquals(outRecords.get(0).value().getHits(), 2);
    }

    @Test
    @DisplayName("Testing punctuation with single key and duplication")
    public void testSingleKeyWithDuplePunctuation() {
        //пишем на вход две записи с одним ключом
        List<TestRecord<String, StatPartDto>> records =
                TestUtils.generateSimilarRecordsWithoutTimestamp(1,4);
        inputTopic.pipeRecordList(records);
        //читаем выходные записи
        List<TestRecord<String, StatOutDto>> outRecords = outputTopic.readRecordsToList();
        //их не должно быть (пунктуации еще не было)
        assertEquals(outRecords.size(), 0);

        //сдвигаем время
        topologyTestDriver.advanceWallClockTime(clockAdvance);
        //пишем на вход запись с другим ключом, после пунктуации
        inputTopic.advanceTime(Duration.ofMillis(clockAdvanceSize - 4 * winAdvanceSize));
        inputTopic.pipeInput(TestUtils.generateRecordWithoutTimestamp(2, 1));
        //читаем выходные записи и печатаем их
        outRecords = outputTopic.readRecordsToList();
        TestUtils.printRecords(outRecords);
        //пунктуация срабатывает раньше обработки новой записи ???
        assertEquals(outRecords.size(), 1);
        assertEquals(outRecords.get(0).key(), TestUtils.genKey(1));
        assertEquals(outRecords.get(0).value().getHits(), 4);
    }

    @Test
    @DisplayName("Testing punctuation with two keys and duplication")
    public void testTwoKeysWithDuplePunctuation() {
        //пишем на вход 3 записи с одним ключом
        List<TestRecord<String, StatPartDto>> records =
                TestUtils.generateSimilarRecordsWithoutTimestamp(1,3);
        inputTopic.pipeRecordList(records);
        //пишем на вход запись с другим ключом, тоже до пунктуации
        inputTopic.pipeInput(TestUtils.generateRecordWithoutTimestamp(2, 1));
        //читаем выходные записи
        List<TestRecord<String, StatOutDto>> outRecords = outputTopic.readRecordsToList();
        //их не должно быть (пунктуации еще не было)
        assertEquals(outRecords.size(), 0);

        //сдвигаем время
        topologyTestDriver.advanceWallClockTime(clockAdvance);
        //пишем на вход запись с другим ключом, после пунктуации
        inputTopic.advanceTime(Duration.ofMillis(clockAdvanceSize - 4 * winAdvanceSize));
        inputTopic.pipeInput(TestUtils.generateRecordWithoutTimestamp(3, 1));
        //читаем выходные записи и печатаем их
        outRecords = outputTopic.readRecordsToList();
        TestUtils.printRecords(outRecords);
        //пунктуация срабатывает раньше обработки новой записи ???
        assertEquals(outRecords.size(), 2); //по числу ключей
        assertEquals(outRecords.get(0).key(), TestUtils.genKey(1));
        assertEquals(outRecords.get(1).key(), TestUtils.genKey(2));
        assertEquals(outRecords.get(0).value().getHits(), 3);
        assertEquals(outRecords.get(1).value().getHits(), 1);
    }

    @Test
    @DisplayName("Testing punctuation with multiple keys and duplication")
    public void testMultipleKeysPunctuation() {
        //пишем на вход две записи с одним ключом
        List<TestRecord<String, StatPartDto>> records =
                TestUtils.generateSimilarRecordsWithoutTimestamp(1,2);
        inputTopic.pipeRecordList(records);
        //читаем выходные записи
        List<TestRecord<String, StatOutDto>> outRecords = outputTopic.readRecordsToList();
        //их не должно быть (пунктуации еще не было)
        assertEquals(outRecords.size(), 0);

        //сдвигаем системное "время"
        topologyTestDriver.advanceWallClockTime(clockAdvance);
        //сдвигаем трек времени во входной теме
        inputTopic.advanceTime(Duration.ofMillis(clockAdvanceSize - 2 * winAdvanceSize));
        //пишем на вход еще три записи с другим ключом
        records = TestUtils.generateSimilarRecordsWithoutTimestamp(2,3);
        inputTopic.pipeRecordList(records);
        //читаем выходные записи
        outRecords = outputTopic.readRecordsToList();
        //должна быть одна запись (прошла первая пунктуация)
        assertEquals(outRecords.size(), 1);
        assertEquals(outRecords.get(0).key(), TestUtils.genKey(1));
        assertEquals(outRecords.get(0).value().getHits(), 2);
        assertEquals(outRecords.get(0).timestamp(),
                startTime.toEpochMilli() + clockAdvanceSize);

        //сдвигаем системное "время" снова
        topologyTestDriver.advanceWallClockTime(clockAdvance);
        //сдвигаем трек времени во входной теме
        inputTopic.advanceTime(Duration.ofMillis(clockAdvanceSize - 3 * winAdvanceSize));
        //пишем на вход запись с другим ключом, после пунктуации
        inputTopic.pipeInput(TestUtils.generateRecordWithoutTimestamp(3, 1));
        //читаем выходные записи и печатаем их
        outRecords = outputTopic.readRecordsToList();
        TestUtils.printRecords(outRecords);
        //пунктуация срабатывает снова и выдает две записи
        assertEquals(outRecords.size(), 2);
        assertEquals(outRecords.get(0).key(), TestUtils.genKey(1));
        assertEquals(outRecords.get(0).value().getHits(), 2);
        assertEquals(outRecords.get(0).timestamp(),
                startTime.toEpochMilli() + 2 * clockAdvanceSize);
        assertEquals(outRecords.get(1).key(), TestUtils.genKey(2));
        assertEquals(outRecords.get(1).value().getHits(), 3);
        assertEquals(outRecords.get(1).timestamp(),
                startTime.toEpochMilli() + 2 * clockAdvanceSize);
    }

    @Test
    @DisplayName("Testing storage with single key and duplication")
    public void testStorageWithSingleKey() {
        //пишем на вход записи с общим ключом и разными ip
        List<TestRecord<String, StatPartDto>> records =
                TestUtils.generateSimilarRecordsWithoutTimestamp(1,4);
        inputTopic.pipeRecordList(records);
        //читаем выходные записи и печатаем их
        List<TestRecord<String, StatOutDto>> outRecords = outputTopic.readRecordsToList();
        TestUtils.printRecords(outRecords);
        //пишем на вход запись с другим ключом, после пунктуации
        inputTopic.pipeInput(TestUtils.generateRecordWithoutTimestamp(2, 1));
        //читаем хранилище и проверяем параметры
        KeyValueStore<String, StatOutDto> store =
                topologyTestDriver.getKeyValueStore(STORE_NAME_SLIDING);
        assertNotNull(store); //оно непустое
        assertEquals(store.name(), STORE_NAME_SLIDING); //проверяем имя
        assertEquals(TestUtils.storeSize(store), 2); //число разных ключей
        //проверяем содержимое хранилища
        String key = TestUtils.genKey(1); //общий ключ
        assertNotNull(store.get(key)); //он должен быть в хранилище
        assertEquals(store.get(key).getHits(), 4); //число событий в последнем окне
        key = TestUtils.genKey(2); //общий ключ
        assertNotNull(store.get(key)); //он должен быть в хранилище
        assertEquals(store.get(key).getHits(), 1); //число событий в последнем окне
    }
}
