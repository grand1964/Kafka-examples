package ru.yandex.grand1964.kafka_demo.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.yandex.grand1964.kafka_demo.dto.StatOutDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;
import ru.yandex.grand1964.kafka_demo.processor.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class StatTopology {
    private static final String TOPIC_NAME = "common";
    private static final String STORE_NAME_SUM = "sumStore";
    private static final String STORE_NAME_DISTINCT = "distinctStore";
    private static final String STORE_NAME_SLIDING = "slidingStore";

    private static final String INPUT_TOPIC_NAME = "common";
    private static final String SINK_TOPIC_NAME = "sink";


    ////////////////////// Тест срабатывания пунктуации //////////////////////

    //TODO УБРАТЬ!!!!!!!!!!!!!!!!
    public static Topology missingTestTopology(Duration punctuationDuration) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KeyValueBytesStoreSupplier distinctStoreSupplier = Stores
                .inMemoryKeyValueStore(STORE_NAME_DISTINCT); //для отсечения дублей
        //TODO ВЕРНУТЬ !!!!!!!!!!!!
        /*KeyValueBytesStoreSupplier slidingStoreSupplier = Stores
                //.persistentKeyValueStore(STORE_NAME_SLIDING); //для данных последнего окна
                .inMemoryKeyValueStore(STORE_NAME_SLIDING); //для данных последнего окна
        */
        //создаем строителей хранилищ
        StoreBuilder<KeyValueStore<String, HashSet<String>>> distinctStoreBuilder = Stores
                .keyValueStoreBuilder(distinctStoreSupplier, Serdes.String(),
                        new JsonSerde<>(HashSet.class));
        /*StoreBuilder<KeyValueStore<String, StatOutDto>> slidingStoreBuilder = Stores
                .keyValueStoreBuilder(slidingStoreSupplier, Serdes.String(),
                        new JsonSerde<>(StatOutDto.class).typeMapper(getTypeMapper()));*/
        //подключаем хранилище типа строка/множество к DSL потока
        streamsBuilder.addStateStore(distinctStoreBuilder);
        //подключаем хранилище статистики типа строка/объект к DSL потока
        //streamsBuilder.addStateStore(slidingStoreBuilder);

        KStream<String, StatOutDto> stream = streamsBuilder.stream(INPUT_TOPIC_NAME,
                        Consumed.with(Serdes.String(), new JsonSerde<>(StatPartDto.class).typeMapper(getTypeMapper()))
                                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                //.withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))

                //.process(() -> new DistinctFilterProcessor(INPUT_TOPIC_NAME), STORE_NAME_DISTINCT)
                //.mapValues(v -> new StatOutDto(INPUT_TOPIC_NAME, v.getUri(), 1))
                .mapValues(v -> new StatOutDto(INPUT_TOPIC_NAME, v.getUri(), Long.parseLong(v.getTimestamp())))
                .filter((k, v) -> !k.contains("mocked"))
                .process(() -> new MissingProcessor(punctuationDuration, "Первый процессор"))
                .groupByKey(Grouped.with(Serdes.String(),
                        new JsonSerde<>(StatOutDto.class)))
                .reduce(
                        (r, v) -> {r.inc(); return r;},
                        Materialized
                                .<String, StatOutDto, KeyValueStore<Bytes, byte[]>>as(STORE_NAME_SUM)
                                .withStoreType(Materialized.StoreType.IN_MEMORY)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(StatOutDto.class)))
                .toStream()
                .process(() -> new MissingProcessor(punctuationDuration, "Второй процессор"))
                .process(() -> new MissingProcessor(punctuationDuration, "Третий процессор"))
                //.process(() -> new MissingProcessor(punctuationDuration, "Четвертый процессор"))
                ;
        //выводим выходные записи
        stream.to(SINK_TOPIC_NAME, Produced.with(
                Serdes.String(),
                new JsonSerde<>(StatOutDto.class)));
        stream.print(Printed.<String, StatOutDto>toSysOut()
                .withLabel("Запись в sink" + "\n ************************************"));
        return streamsBuilder.build();
    }

    //TODO УБРАТЬ!!!!!!!!!!!!!!!!
    public static Topology punctuationTestTopology(Duration punctuationDuration) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //TODO УБРАТЬ!!!!!!!!!!
        /*//поставщик хранилища в памяти
        KeyValueBytesStoreSupplier slidingStoreSupplier = Stores
                .inMemoryKeyValueStore("testStore");
        //создаем строителя хранилища
        StoreBuilder<KeyValueStore<String, StatOutDto>> slidingStoreBuilder = Stores
                .keyValueStoreBuilder(slidingStoreSupplier, Serdes.String(),
                        new JsonSerde<>(StatOutDto.class).typeMapper(getTypeMapper()));
        //подключаем хранилище типа строка/объект статистики к DSL потока
        streamsBuilder.addStateStore(slidingStoreBuilder);*/

        KStream<String, StatOutDto> stream = streamsBuilder.stream(TOPIC_NAME,
                Consumed.with(Serdes.String(), new JsonSerde<>(StatPartDto.class).typeMapper(getTypeMapper()))
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                        //.withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                .mapValues(v -> new StatOutDto(TOPIC_NAME, v.getUri(), Long.parseLong(v.getTimestamp())))
                .process(() -> new PunctuatorTestProcessor(punctuationDuration));

        /*KStream<String, StatOutDto> stream = streamsBuilder.stream(TOPIC_NAME,
                        Consumed.with(Serdes.String(), new JsonSerde<>(StatPartDto.class).typeMapper(getTypeMapper()))
                                //.withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                .mapValues(v -> new StatOutDto(TOPIC_NAME, v.getUri(), Long.parseLong(v.getTimestamp())))
                //TODO ВЕРНУТЬ!!!
                //.process(() -> new PunctuatorTestProcessor(punctuationDuration), "testStore");
                .process(() -> new PunctuatorTestProcessor(punctuationDuration));*/
        //выводим выходные записи
        stream.to("sink", Produced.with(
                Serdes.String(),
                new JsonSerde<>(StatOutDto.class)));
        stream.print(Printed.<String, StatOutDto>toSysOut()
                .withLabel("Запись в sink"));
        return streamsBuilder.build();
    }

    //TODO УБРАТЬ!!!!!!
    public static Topology punctuationTestWithDupleTopology(
            Duration winSize, Duration punctuationDuration) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //поставщик хранилища в памяти
        KeyValueBytesStoreSupplier slidingStoreSupplier = Stores
                .inMemoryKeyValueStore(STORE_NAME_SLIDING);
        //создаем строителя хранилища
        StoreBuilder<KeyValueStore<Windowed<String>, StatOutDto>> slidingStoreBuilder = Stores
                .keyValueStoreBuilder(slidingStoreSupplier,
                        WindowedSerdes.timeWindowedSerdeFrom(String.class,winSize.toMillis()),
                        new JsonSerde<>(StatOutDto.class).typeMapper(getTypeMapper()));
        //подключаем хранилище типа строка/объект статистики к DSL потока
        streamsBuilder.addStateStore(slidingStoreBuilder);

        //создаем входной поток и подключаем его к теме
        KStream<String, StatPartDto> streamIn = streamsBuilder.stream(TOPIC_NAME,
                Consumed.with(Serdes.String(), new JsonSerde<>(StatPartDto.class)
                                .typeMapper(getTypeMapper()))
                        //.withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));
        KStream<Windowed<String>, StatOutDto> streamOut = streamIn
                .mapValues(v -> new StatOutDto(TOPIC_NAME, v.getUri(), 1L))
                .groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(winSize))
                //агрегируем счетчик суммированием
                .reduce((r, v) -> {r.inc(); return r;},
                        Materialized
                                .<String, StatOutDto, WindowStore<Bytes, byte[]>>as(STORE_NAME_SUM)
                                .withStoreType(Materialized.StoreType.IN_MEMORY)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(StatOutDto.class)))
                .toStream()

                //TODO ПРОВЕРИТЬ !!!!!!!!!!!
                //сохраняем записи
                //.process(StoreProcessor::new, STORE_NAME_SLIDING)
                //выполняем пунктуацию
                .process(() -> new PunctuationWindowedProcessor(punctuationDuration), STORE_NAME_SLIDING);
        //.process(() -> new PunctuationWithoutStoreProcessor(punctuationDuration), STORE_NAME_SLIDING);

        //выводим выходные записи
        streamOut.to("sink", Produced.with(
                WindowedSerdes.timeWindowedSerdeFrom(String.class,winSize.toMillis()),
                //Serdes.String(),
                 new JsonSerde<>(StatOutDto.class)));
        streamOut.print(Printed.<Windowed<String>, StatOutDto>toSysOut()
                .withLabel("Запись в sink"));
        return streamsBuilder.build();
    }

    //TODO УБРАТЬ!!!!!!
    public static Topology punctuationWithGrouppingTopology(Duration punctuationDuration) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //поставщик хранилища в памяти
        /*KeyValueBytesStoreSupplier slidingStoreSupplier = Stores
                .inMemoryKeyValueStore(STORE_NAME_SLIDING);
        //создаем строителя хранилища
        StoreBuilder<KeyValueStore<String, StatOutDto>> slidingStoreBuilder = Stores
                .keyValueStoreBuilder(slidingStoreSupplier,
                        Serdes.String(),
                        new JsonSerde<>(StatOutDto.class).typeMapper(getTypeMapper()));
        //подключаем хранилище типа строка/объект статистики к DSL потока
        streamsBuilder.addStateStore(slidingStoreBuilder);*/

        //создаем входной поток и подключаем его к теме
        KStream<String, StatPartDto> streamIn = streamsBuilder.stream(TOPIC_NAME,
                Consumed.with(Serdes.String(), new JsonSerde<>(StatPartDto.class)
                                .typeMapper(getTypeMapper()))
                        //.withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));
        KStream<String, StatOutDto> streamOut = streamIn
                .mapValues(v -> new StatOutDto(TOPIC_NAME, v.getUri(), 1L))
                .groupByKey()
                //агрегируем счетчик суммированием
                .reduce((r, v) -> {r.inc(); return r;},
                        Materialized
                                .<String, StatOutDto, KeyValueStore<Bytes, byte[]>>as(STORE_NAME_SUM)
                                .withStoreType(Materialized.StoreType.IN_MEMORY)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(StatOutDto.class)))
                .toStream()
                //выполняем пунктуацию
                .process(() -> new PunctuationGroupProcessor(punctuationDuration));
        //.process(() -> new PunctuationWithoutStoreProcessor(punctuationDuration), STORE_NAME_SLIDING);

        //выводим выходные записи
        streamOut.to("sink", Produced.with(
                Serdes.String(),
                new JsonSerde<>(StatOutDto.class)));
        streamOut.print(Printed.<String, StatOutDto>toSysOut()
                .withLabel("Запись в sink"));
        return streamsBuilder.build();
    }

    //////////////////////////////////////////////////////////////////////////
    ///////////////////// Генерация топологий статистики /////////////////////
    //////////////////////////////////////////////////////////////////////////

    ///////////////////// С пунктуацией без дублирования /////////////////////

    public static Topology buildCountDistinctWithPunctuationTopology(
            Duration winSize, Duration punctuationDuration) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        //поставщики хранилищ в памяти
        KeyValueBytesStoreSupplier distinctStoreSupplier = Stores
                .inMemoryKeyValueStore(STORE_NAME_DISTINCT);
        KeyValueBytesStoreSupplier slidingStoreSupplier = Stores
                .inMemoryKeyValueStore(STORE_NAME_SLIDING);
        //создаем строителей хранилищ
        StoreBuilder<KeyValueStore<String, HashSet<String>>> distinctStoreBuilder = Stores
                .keyValueStoreBuilder(distinctStoreSupplier, Serdes.String(),
                        new JsonSerde<>(HashSet.class));
        StoreBuilder<KeyValueStore<String, StatOutDto>> slidingStoreBuilder = Stores
                .keyValueStoreBuilder(slidingStoreSupplier, Serdes.String(),
                        new JsonSerde<>(StatOutDto.class).typeMapper(getTypeMapper()));
        //подключаем хранилище типа строка/множество к DSL потока
        streamsBuilder.addStateStore(distinctStoreBuilder);
        //подключаем хранилище типа строка/объект статистики к DSL потока
        streamsBuilder.addStateStore(slidingStoreBuilder);
        //создаем входной поток и подключаем его к теме
        KStream<String, StatPartDto> streamIn = streamsBuilder.stream(TOPIC_NAME,
                Consumed.with(Serdes.String(),
                                new JsonSerde<>(StatPartDto.class)
                                        .typeMapper(getTypeMapper()))
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
        KStream<String, StatOutDto> streamOut = streamIn
                //отсекаем дубли и преобразуем в выходной тип
                .process(() -> new DistinctFilterProcessor(TOPIC_NAME),
                        STORE_NAME_DISTINCT)
                //группировка по окнам (с обязательным указанием Serde!)
                .groupByKey(Grouped.with(Serdes.String(),
                        new JsonSerde<>(StatOutDto.class)))
                //собираем по скользящим окнам
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(winSize))
                //агрегируем счетчик суммированием
                .reduce(
                        (r, v) -> {r.inc(); return r;},
                        Materialized
                                .<String, StatOutDto, WindowStore<Bytes, byte[]>>as(STORE_NAME_SUM)
                                .withStoreType(Materialized.StoreType.IN_MEMORY)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(StatOutDto.class)))
                .toStream()

                //TODO ПРОВЕРИТЬ !!!!!!!!!!!
                //сохраняем записи
                .process(StoreProcessor::new, STORE_NAME_SLIDING)
                //выполняем пунктуацию
                .process(() -> new PunctuationWithoutStoreProcessor(punctuationDuration), STORE_NAME_SLIDING);
                //.process(() -> new PunctuationWithoutStoreProcessor(punctuationDuration), STORE_NAME_SLIDING);

        //выводим выходные записи
        streamOut.to("sink", Produced.with(
                Serdes.String(),
                new JsonSerde<>(StatOutDto.class)));
        streamOut.print(Printed.<String, StatOutDto>toSysOut()
                .withLabel("Запись в sink"));
        return streamsBuilder.build();
    }

    ////////////////////// С пунктуацией и дублированием /////////////////////

    //Оконная версия с повторами для "скользящих" (Sliding) окон
    public static Topology buildCountDupleWithPunctuationTopology(
            Duration winSize, Duration punctuationDuration) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //поставщик хранилища в памяти
        KeyValueBytesStoreSupplier slidingStoreSupplier = Stores
                .inMemoryKeyValueStore(STORE_NAME_SLIDING);
        //создаем строителя хранилища
        StoreBuilder<KeyValueStore<String, StatOutDto>> slidingStoreBuilder = Stores
                .keyValueStoreBuilder(slidingStoreSupplier, Serdes.String(),
                        new JsonSerde<>(StatOutDto.class).typeMapper(getTypeMapper()));
        //подключаем хранилище типа строка/объект статистики к DSL потока
        streamsBuilder.addStateStore(slidingStoreBuilder);

        //создаем входной поток и подключаем его к теме
        KStream<String, StatPartDto> streamIn = streamsBuilder.stream(TOPIC_NAME,
                Consumed.with(Serdes.String(), new JsonSerde<>(StatPartDto.class)
                                .typeMapper(getTypeMapper()))
                        //.withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));
        KStream<String, StatOutDto> streamOut = streamIn
                .mapValues(v -> new StatOutDto(TOPIC_NAME, v.getUri(), 1L))
                .groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(winSize))
                //агрегируем счетчик суммированием
                .reduce((r, v) -> {r.inc(); return r;},
                        Materialized
                                .<String, StatOutDto, WindowStore<Bytes, byte[]>>as(STORE_NAME_SUM)
                                .withStoreType(Materialized.StoreType.IN_MEMORY)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(StatOutDto.class)))
                .toStream()

                //TODO ПРОВЕРИТЬ !!!!!!!!!!!
                //сохраняем записи
                //.process(StoreProcessor::new, STORE_NAME_SLIDING)
                //выполняем пунктуацию
                .process(() -> new PunctuationProcessor(punctuationDuration), STORE_NAME_SLIDING);
                //.process(() -> new PunctuationWithoutStoreProcessor(punctuationDuration), STORE_NAME_SLIDING);

        //выводим выходные записи
        streamOut.to("sink", Produced.with(
                Serdes.String(), new JsonSerde<>(StatOutDto.class)));
        streamOut.print(Printed.<String, StatOutDto>toSysOut()
                .withLabel("Запись в sink"));
        return streamsBuilder.build();
    }

    //////////////////////////////////////////////////////////////////////////
    //////////////////////////// Информация о типах //////////////////////////
    //////////////////////////////////////////////////////////////////////////

    private static DefaultJackson2JavaTypeMapper getTypeMapper() {
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
}


