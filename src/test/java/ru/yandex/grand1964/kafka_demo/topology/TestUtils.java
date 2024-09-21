package ru.yandex.grand1964.kafka_demo.topology;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import ru.yandex.grand1964.kafka_demo.dto.StatOutDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;

public class TestUtils {

    /////////////////////////// Генерация параметров /////////////////////////

    public static String genKey(int num) {
        return "/event/" + num;
    }

    public static String genIp(int num) {
        return "192.168.17." + num;
    }

    ///////////////////////////// Генерация записей //////////////////////////

    //одиночная запись с меткой времени
    public static TestRecord<String, StatPartDto> generateRecord(
            int keyId, int ipId, long timestamp) {
        String key = genKey(keyId);
        StatPartDto v = new StatPartDto(key, genIp(ipId), Long.toString(timestamp));
        return new TestRecord<>(key, v, Instant.ofEpochMilli(timestamp));
    }

    //фиктивная одиночная запись с меткой времени
    public static TestRecord<String, StatPartDto> generateMockRecord(long timestamp) {
        StatPartDto v = new StatPartDto("", "", Long.toString(timestamp));
        return new TestRecord<>("", v, Instant.ofEpochMilli(timestamp));
    }

    //одиночная запись с автоматически генерируемой меткой времени
    public static TestRecord<String, StatPartDto> generateRecordWithoutTimestamp(
            int keyId, int ipId) {
        String key = genKey(keyId);
        StatPartDto v = new StatPartDto(key, genIp(ipId), null);
        return new TestRecord<>(key, v);
    }

    //группа записей с одним ключом, разными ip и автоматически генерируемыми метками времени
    public static List<TestRecord<String, StatPartDto>> generateDistinctRecordsWithoutTimestamp(
            int keyId, int count) {
        List<TestRecord<String, StatPartDto>> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String key = genKey(keyId);
            result.add(new TestRecord<>(key,
                    new StatPartDto(key, genIp(i),null)));
        }
        return result;
    }

    //группа записей с одинаковыми ключом/ip и автоматически генерируемыми метками времени
    public static List<TestRecord<String, StatPartDto>> generateSimilarRecordsWithoutTimestamp(
            int keyId, int count) {
        List<TestRecord<String, StatPartDto>> result = new ArrayList<>();
        String key = genKey(keyId);
        String ip = genIp(1);
        for (int i = 0; i < count; i++) {
            result.add(new TestRecord<>(key,
                    new StatPartDto(key, ip, null)));
        }
        return result;
    }

    //////////////////////////// Конвертер времени ///////////////////////////

    //преобразование данных Instant во время с "российским" смещением
    public static OffsetDateTime toDateTime(long timestamp) {
        return OffsetDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp), ZoneId.of("UTC+03:00"));
    }

    /////////////////////////////// Вывод данных /////////////////////////////

    //вычисление количества записей в хранилище
    public static int storeSize(KeyValueStore<?,?> store) {
        int result = 0;
        try ( KeyValueIterator<?, ?> iterator = store.all()) {
            while (iterator.hasNext()) {
                result++;
                iterator.next();
            }
        }
        return result;
    }

    //печать списка записей (значение и метка времени)
    public static void printRecords(List<TestRecord<String, StatOutDto>> records) {
        for (TestRecord<String, StatOutDto> record : records) {
            System.out.println("Received: " + record.getValue());
            System.out.println("Received timestamp=" + record.timestamp() +
                " -> " + toDateTime(record.timestamp()));
        }
    }

    /////////////////////////// Информация о типах ///////////////////////////

    public static DefaultJackson2JavaTypeMapper getTypeMapper() {
        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
        typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.TYPE_ID);
        typeMapper.addTrustedPackages("ru.yandex.grand1964.kafka_demo.dto");
        Map<String, Class<?>> mappings = new HashMap<>();
        mappings.put("full", ru.yandex.grand1964.kafka_demo.dto.StatInDto.class);
        mappings.put("part", ru.yandex.grand1964.kafka_demo.dto.StatPartDto.class);
        mappings.put("out", ru.yandex.grand1964.kafka_demo.dto.StatOutDto.class);
        typeMapper.setIdClassMapping(mappings);
        return typeMapper;
    }

    /////////////////////////// Настройки драйвера ///////////////////////////

    public static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "app.1");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                FailOnInvalidTimestamp.class); //метка - из записи, а не системное время!
        //режим CREATE_TIME задается по умолчанию
        return props;
    }
}
