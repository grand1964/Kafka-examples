package ru.yandex.grand1964.kafka_demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.dto.StatOutDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.IntFunction;

public class TestUtils {

    /////////////////////////// Генерация параметров /////////////////////////

    public static String genKey(int num) {
        return "/event/" + num;
    }

    public static String genIp(int num) {
        return "192.168.17." + num;
    }

    /////////////////////// Генерация входных объектов ///////////////////////

    //создание входного объекта
    public static StatInDto generateInputObject(
            String app, int keyId, int ipId, LocalDateTime timestamp, String timePattern) {
        String key = genKey(keyId);
        return new StatInDto(app, key, genIp(ipId), formatDateTime(timestamp, timePattern));
    }

    public static StatInDto generateInputObject(
            String app, int keyId, int ipId, LocalDateTime timestamp) {
        String key = genKey(keyId);
        return new StatInDto(app, key, genIp(ipId), Long.toString(fromDateTimeToMillis(timestamp)));
    }

    public static StatInDto generateMockedInputObject(
            String app, LocalDateTime timestamp, int index) {
        return new StatInDto(app, "mocked" + index, "", Long.toString(fromDateTimeToMillis(timestamp)));
    }

    ////////////////////////// Генерация сообщений ///////////////////////////

    //преобразование входного объекта в сообщение с преобразованием времени
    public static Message<StatPartDto> fromStatInDto(StatInDto statInDto, String timePattern) {
        //имя темы - это имя приложения с префиксом
        String topicName = statInDto.getApp();
        //генерируем dto без имени приложения
        StatPartDto statPartDto = statInDto.toPartDto();
        //преобразуем время из вложения в UNIX-формат
        long dateMillis = parseTimestampToLong(statInDto.getTimestamp(), timePattern);
        /*DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timePattern);
        long dateMillis = LocalDateTime.parse(statInDto.getTimestamp(), formatter)
                .toInstant(ZoneOffset.of("+03:00"))
                .toEpochMilli();*/
        return MessageBuilder.withPayload(statPartDto)
                .setHeader(KafkaHeaders.TOPIC, topicName)
                .setHeader(KafkaHeaders.KEY, statInDto.getUri())
                //TODO ВЕРНУТЬ !!!!!!!!!!!!!!
                .setHeader(KafkaHeaders.TIMESTAMP, dateMillis)
                .setHeader(KafkaHeaders.PARTITION, 0)
                //время назначает система
                .build();
    }

    //преобразование в сообщение входного объекта с миллисекундами
    public static Message<StatPartDto> fromStatInDto(StatInDto statInDto) {
        //имя темы - это имя приложения с префиксом
        String topicName = statInDto.getApp();
        //генерируем dto без имени приложения
        StatPartDto statPartDto = statInDto.toPartDto();
        //преобразуем время из вложения в UNIX-формат
        long dateMillis = Long.parseLong(statInDto.getTimestamp());
        return MessageBuilder.withPayload(statPartDto)
                .setHeader(KafkaHeaders.TOPIC, topicName)
                .setHeader(KafkaHeaders.KEY, statInDto.getUri())
                .setHeader(KafkaHeaders.TIMESTAMP, dateMillis)
                .setHeader(KafkaHeaders.PARTITION, 0)
                //время назначает система
                .build();
    }

    public static List<Message<StatPartDto>> generateMockedTimestamps(String topic, int count,
                           IntFunction<Integer> times, IntFunction<Integer> uris, IntFunction<Integer> ips) {
        List<Message<StatPartDto>> messages = new ArrayList<>();
        LocalDateTime timestamp = TestUtils.toDateTime(0L).toLocalDateTime();
        for (int i = 0; i < count; i++) {
            StatInDto dto = TestUtils.generateInputObject(
                    topic, uris.apply(i), ips.apply(i), timestamp.plusSeconds(times.apply(i)));
            messages.add(TestUtils.fromStatInDto(dto));
        }
        return messages;
    }

    public static List<Message<StatPartDto>> generateMockedTimestamps(
            String topic, int[] times, IntFunction<Integer> uris, IntFunction<Integer> ips) {
        return generateMockedTimestamps(topic, times.length, i -> times[i], uris, ips);
    }

    public static List<Message<StatPartDto>> generateMockedTimestamps(
            String topic, int[] times, IntFunction<Integer> uris) {
        return generateMockedTimestamps(topic, times, uris, i -> i);
    }

    public static List<Message<StatPartDto>> generateMockedTimestamps(String topic, int[] times) {
        return generateMockedTimestamps(topic, times, i -> 1);
    }

    public static List<Message<StatPartDto>> generateMockedTimestamps(
            String topic, int[] times, int[] uris, IntFunction<Integer> ips) {
        if (times.length != uris.length) {
            throw new RuntimeException("Длины массивов times и uris должны совпадать!");
        }
        return generateMockedTimestamps(topic, times.length, i -> times[i], i -> uris[i], ips);
    }

    public static List<Message<StatPartDto>> generateMockedTimestamps(
            String topic, int[] times, int[] uris) {
        return generateMockedTimestamps(topic, times, uris, i -> 1);
    }

    public static List<Message<StatPartDto>> generateMockedTimestamps(
            String topic, int[] times, int[] uris, int[] ips) {
        if (times.length != uris.length) {
            throw new RuntimeException("Длины массивов times и uris должны совпадать!");
        }
        if (times.length != ips.length) {
            throw new RuntimeException("Длины массивов times и ips должны совпадать!");
        }
        return generateMockedTimestamps(topic, times.length, i -> times[i], i -> uris[i], i -> ips[i]);
    }

    //////////////////////////// Генерация записей ///////////////////////////

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

    /*public static Message<StatPartDto> generateMockedMessage(
            String topicName, LocalDateTime time, String timePattern) {
        String timestamp = formatDateTime(time, timePattern);
        //генерируем dto
        StatPartDto statPartDto = new StatPartDto("","", timestamp);
        //преобразуем время из вложения в UNIX-формат
        return MessageBuilder.withPayload(statPartDto)
                .setHeader(KafkaHeaders.TOPIC, topicName)
                .setHeader(KafkaHeaders.KEY, "")
                .setHeader(KafkaHeaders.TIMESTAMP, timestamp)
                .setHeader(KafkaHeaders.PARTITION, 0)
                .build();
    }*/

    /////////////////////////// Конвертеры времени ///////////////////////////

    //////конвертация timestamp в миллисекундах во время с "российским" смещением

    public static OffsetDateTime toDateTime(long timestamp) {
        return OffsetDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp), ZoneId.of("UTC+03:00"));
    }

    ////// Конвертация локального времени с "российским" смещением:

    //в миллисекунды
    public static long fromDateTimeToMillis(LocalDateTime timestamp) {
        return timestamp.atOffset(ZoneOffset.ofHours(3)).toEpochSecond() * 1000;
    }

    //в Instant
    public static Instant fromDateTime(LocalDateTime timestamp) {
        return Instant.ofEpochMilli(fromDateTimeToMillis(timestamp));
        //return Instant.ofEpochMilli(timestamp.atOffset(ZoneOffset.ofHours(3)).toEpochSecond() * 1000);
    }

    //в строку заданного формата
    public static String formatDateTime(LocalDateTime dateTime, String timePattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timePattern);
        return formatter.format(dateTime);
    }

    //////конвертация строки-времени с "российским" смещением в миллисекунды

    public static long parseTimestampToLong(String timestamp, String timePattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timePattern);
        return LocalDateTime.parse(timestamp, formatter)
                .toInstant(ZoneOffset.of("+03:00"))
                .toEpochMilli();
    }

    //////выравнивание времени на границу кратного секундам
    public static LocalDateTime alignTimestamp(LocalDateTime timestamp, int multiple) {
        int step = 1000 * multiple;
        long t = fromDateTimeToMillis(timestamp) / step * step;
        return toDateTime(t).toLocalDateTime();
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
