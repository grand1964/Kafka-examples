package ru.yandex.grand1964.kafka_demo.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.lang.NonNull;
import ru.yandex.grand1964.kafka_demo.dto.StatOutDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;
import ru.yandex.grand1964.kafka_demo.processor.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class StatBuilderRelease implements KafkaStreamsInfrastructureCustomizer {
    //TODO Вернуть private и static

    @Value("${topic.prefix}${input.topic.name}")
    public String INPUT_TOPIC_NAME;
    @Value("${sink.topic.name}")
    public String SINK_TOPIC_NAME;
    @Value("${stat.unique.ip}")
    public boolean defaultUniqueIp;
    public Boolean uniqueIp;
    @Value("#{T(java.time.Duration).ofMillis(${stat.window.size})}")
    public Duration windowSizeInMs;
    @Value("#{T(java.time.Duration).ofMillis(${stat.punctuation.step})}")
    public Duration punctuationStepInMs;

    //TODO Досюда

    private static final String STORE_NAME_SUM = "sumStore";
    private static final String STORE_NAME_DISTINCT = "distinctStore";
    private static final String STORE_NAME_SLIDING = "slidingStore";

    public StatBuilderRelease(Boolean uniqueIp) {
        this.uniqueIp = uniqueIp;
    }

    //////////////////////////////////////////////////////////////////////////
    ///////////// Реализация KafkaStreamsInfrastructureCustomizer ////////////
    //////////////////////////////////////////////////////////////////////////

    @Override
    public void configureBuilder(@NonNull StreamsBuilder streamsBuilder) {
        //TODO ВЕРНУТЬ!!!!!!!!!!!!!!
        //testMissingBuilder(streamsBuilder);
        //testBuilder(streamsBuilder);
        //testDistinctBuilder(streamsBuilder, windowSizeInMs, punctuationStepInMs);

        boolean unique;
        if (uniqueIp == null) {
            unique = defaultUniqueIp;
        } else {
            unique = uniqueIp;
        }
        if (unique) {
            countDistinctPunctuationBuilder(streamsBuilder, windowSizeInMs, punctuationStepInMs);
        } else {
            countWithDuplePunctuationBuilder(streamsBuilder, windowSizeInMs, punctuationStepInMs);
        }
    }

    //второй метод не реализуем

    //////////////////////////////////////////////////////////////////////////
    ///////////////////// Конфигурация билдера статистики ////////////////////
    //////////////////////////////////////////////////////////////////////////

    //TODO Тест, УБРАТЬ !!!!!!!!!!!!
    private void testMissingBuilder(StreamsBuilder streamsBuilder) {
        /*//поставщик хранилищ в памяти
        KeyValueBytesStoreSupplier testStoreSupplier = Stores
                //.persistentKeyValueStore("testStore");
                .inMemoryKeyValueStore("testStore");
        //создаем строителей хранилищ
        StoreBuilder<KeyValueStore<String, ArrayList<Long>>> testStoreBuilder = Stores
                .keyValueStoreBuilder(testStoreSupplier, Serdes.String(),
                        new JsonSerde<>(ArrayList.class));
        //подключаем хранилище типа строка/множество к DSL потока
        streamsBuilder.addStateStore(testStoreBuilder);*/

        KStream<String, StatOutDto> stream = streamsBuilder.stream(INPUT_TOPIC_NAME,
                        Consumed.with(Serdes.String(), new JsonSerde<>(StatPartDto.class).typeMapper(getTypeMapper()))
                                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                //.withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                .mapValues(v -> new StatOutDto(INPUT_TOPIC_NAME, v.getUri(), Long.parseLong(v.getTimestamp())))
                .filter((k, v) -> !k.contains("mocked"))
                .process(() -> new MissingProcessor(punctuationStepInMs, "Первый процессор"))
                .process(() -> new MissingProcessor(punctuationStepInMs, "Второй процессор"));
        //выводим выходные записи
        stream.to(SINK_TOPIC_NAME, Produced.with(
                Serdes.String(),
                new JsonSerde<>(StatOutDto.class)));
        stream.print(Printed.<String, StatOutDto>toSysOut()
                .withLabel("Запись в sink" + "\n ************************************"));
    }

    //TODO Тест, УБРАТЬ !!!!!!!!!!!!
    private void testBuilder(StreamsBuilder streamsBuilder) {
        /*//поставщик хранилищ в памяти
        KeyValueBytesStoreSupplier testStoreSupplier = Stores
                //.persistentKeyValueStore("testStore");
                .inMemoryKeyValueStore("testStore");
        //создаем строителей хранилищ
        StoreBuilder<KeyValueStore<String, ArrayList<Long>>> testStoreBuilder = Stores
                .keyValueStoreBuilder(testStoreSupplier, Serdes.String(),
                        new JsonSerde<>(ArrayList.class));
        //подключаем хранилище типа строка/множество к DSL потока
        streamsBuilder.addStateStore(testStoreBuilder);*/

        KStream<String, StatOutDto> stream = streamsBuilder.stream(INPUT_TOPIC_NAME,
                Consumed.with(Serdes.String(), new JsonSerde<>(StatPartDto.class).typeMapper(getTypeMapper()))
                        //.withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                //.mapValues(v -> new StatOutDto(INPUT_TOPIC_NAME, v.getUri(), 17L))
                .mapValues(v -> new StatOutDto(INPUT_TOPIC_NAME, v.getUri(), Long.parseLong(v.getTimestamp())))
                //TODO ВЕРНУТЬ!!!!!!!
                //.process(() -> new PunctuatorTestProcessor(punctuationStepInMs), "testStore");
                .process(() -> new PunctuatorTestProcessor(punctuationStepInMs));
        //выводим выходные записи
        stream.to(SINK_TOPIC_NAME, Produced.with(
                Serdes.String(),
                new JsonSerde<>(StatOutDto.class)));
        stream.print(Printed.<String, StatOutDto>toSysOut()
                .withLabel("Запись в sink"));
    }


    ///////////////////// С пунктуацией без дублирования /////////////////////

    //TODO ТЕСТОВЫЙ, УБРАТЬ !!!!!
    private void testDistinctBuilder(StreamsBuilder streamsBuilder,
                                                 Duration winSize, Duration punctuationDuration) {
        //поставщики хранилищ в памяти
        KeyValueBytesStoreSupplier distinctStoreSupplier = Stores
                //.persistentKeyValueStore(STORE_NAME_DISTINCT); //для отсечения дублей
                .inMemoryKeyValueStore(STORE_NAME_DISTINCT); //для отсечения дублей
        //TODO ВЕРНУТЬ !!!!!!!!!!!!
        /*KeyValueBytesStoreSupplier slidingStoreSupplier = Stores
                //.persistentKeyValueStore(STORE_NAME_SLIDING); //для данных последнего окна
                .inMemoryKeyValueStore(STORE_NAME_SLIDING); //для данных последнего окна*/
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

        //создаем входной поток и подключаем его к теме
        KStream<String, StatOutDto> stream = streamsBuilder.stream(INPUT_TOPIC_NAME,
                        Consumed.with(Serdes.String(), new JsonSerde<>(StatPartDto.class).typeMapper(getTypeMapper()))
                                //TODO ????????????? ПРОВЕРИТЬ !!!!!
                                //.withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                //отсекаем дубли и преобразуем данные в выходной тип
                .process(SimplePrintProcessor::new)
                .process(() -> new PseudoDistinctProcessor(INPUT_TOPIC_NAME), STORE_NAME_DISTINCT)
                //.process(() -> new PseudoDistinctProcessor(INPUT_TOPIC_NAME))
                //.process(() -> new DistinctFilterProcessor(INPUT_TOPIC_NAME), STORE_NAME_DISTINCT)
                //.mapValues(v -> new StatOutDto(INPUT_TOPIC_NAME, v.getUri(), 1))
                ;

        //выводим выходные записи
        stream.to(SINK_TOPIC_NAME, Produced.with(
                Serdes.String(),
                new JsonSerde<>(StatOutDto.class)));
        stream.print(Printed.<String, StatOutDto>toSysOut()
                .withLabel("Запись в sink"));
    }

    //TODO ОСНОВНАЯ ВЕРСИЯ, ВЕРНУТЬ !!!!!!!

    private void countDistinctPunctuationBuilder(StreamsBuilder streamsBuilder,
                                                     Duration winSize, Duration punctuationDuration) {
        //поставщики хранилищ в памяти
        KeyValueBytesStoreSupplier distinctStoreSupplier = Stores
                //.persistentKeyValueStore(STORE_NAME_DISTINCT); //для отсечения дублей
                .inMemoryKeyValueStore(STORE_NAME_DISTINCT); //для отсечения дублей
        //TODO ВЕРНУТЬ !!!!!!!!!!!!
        KeyValueBytesStoreSupplier slidingStoreSupplier = Stores
                //.persistentKeyValueStore(STORE_NAME_SLIDING); //для данных последнего окна
                .inMemoryKeyValueStore(STORE_NAME_SLIDING); //для данных последнего окна
        //создаем строителей хранилищ
        StoreBuilder<KeyValueStore<String, HashSet<String>>> distinctStoreBuilder = Stores
                .keyValueStoreBuilder(distinctStoreSupplier, Serdes.String(),
                        new JsonSerde<>(HashSet.class));
        StoreBuilder<KeyValueStore<String, StatOutDto>> slidingStoreBuilder = Stores
                .keyValueStoreBuilder(slidingStoreSupplier, Serdes.String(),
                        new JsonSerde<>(StatOutDto.class).typeMapper(getTypeMapper()));
        //подключаем хранилище типа строка/множество к DSL потока
        streamsBuilder.addStateStore(distinctStoreBuilder);
        //подключаем хранилище статистики типа строка/объект к DSL потока
        streamsBuilder.addStateStore(slidingStoreBuilder);

        //создаем входной поток и подключаем его к теме
        KStream<String, StatOutDto> stream = streamsBuilder.stream(INPUT_TOPIC_NAME,
                    Consumed.with(Serdes.String(), new JsonSerde<>(StatPartDto.class).typeMapper(getTypeMapper()))
                            //TODO ????????????? ПРОВЕРИТЬ !!!!!
                //.withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                //отсекаем дубли и преобразуем данные в выходной тип
                .process(() -> new DistinctFilterProcessor(INPUT_TOPIC_NAME),
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
                //TODO ВЕРНУТЬ !!!!!!!!!!!!!!

                //сохраняем записи
                //.process(StoreProcessor::new, STORE_NAME_SLIDING);
                //выполняем пунктуацию
                .process(() -> new PunctuationProcessor(punctuationDuration), STORE_NAME_SLIDING);
                //.process(() -> new PunctuationWithoutStoreProcessor(punctuationDuration), STORE_NAME_SLIDING);

        //выводим выходные записи
        stream
                //.process(PrintProcessor::new, STORE_NAME_SLIDING)
                //.process(() -> new PunctuationWithoutStoreProcessor(punctuationDuration), STORE_NAME_SLIDING)
                .to(SINK_TOPIC_NAME, Produced.with(
              Serdes.String(),
              new JsonSerde<>(StatOutDto.class)));
        stream.print(Printed.<String, StatOutDto>toSysOut()
              .withLabel("Запись в sink"));
    }

    ////////////////////// С пунктуацией и дублированием /////////////////////

    //Оконная версия с повторами для "скользящих" (Sliding) окон
    private void countWithDuplePunctuationBuilder(StreamsBuilder streamsBuilder,
                                                  Duration winSize, Duration punctuationDuration) {
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
        KStream<String, StatOutDto> stream = streamsBuilder.stream(INPUT_TOPIC_NAME,
                    Consumed.with(Serdes.String(), new JsonSerde<>(StatPartDto.class).typeMapper(getTypeMapper()))
                            //TODO ВЕРНУТЬ LATEST !!!!
                //.withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                //преобразуем данные в выходной тип
                .mapValues(v -> new StatOutDto(INPUT_TOPIC_NAME, v.getUri(), 1L))
                //группировка по окнам (с обязательным указанием Serde!)
                .groupByKey()
                //собираем по скользящим окнам
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(winSize))
                //агрегируем счетчик суммированием
                .reduce((r, v) -> {r.inc(); return r;},
                        Materialized
                                .<String, StatOutDto, WindowStore<Bytes, byte[]>>as(STORE_NAME_SUM)
                                .withStoreType(Materialized.StoreType.IN_MEMORY)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(StatOutDto.class)))
                .toStream()

                //TODO ПРОВЕРИТЬ!!!!!!!!!!
                //сохраняем записи
                .process(() -> new PunctuationProcessor(punctuationDuration), STORE_NAME_SLIDING);
                //.process(StoreProcessor::new, STORE_NAME_SLIDING)
                //выполняем пунктуацию
                //.process(() -> new PunctuationWithoutStoreProcessor(punctuationDuration), STORE_NAME_SLIDING);
                //.process(() -> new PunctuationWithoutStoreProcessor(punctuationDuration), STORE_NAME_SLIDING);


        //выводим и печатаем выходные записи
        stream
                .to(SINK_TOPIC_NAME, Produced.with(Serdes.String(), new JsonSerde<>(StatOutDto.class)));
        stream.print(Printed.<String, StatOutDto>toSysOut()
                .withLabel("Запись в sink"));
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
        mappings.put("part", StatPartDto.class);
        mappings.put("out", StatOutDto.class);
        typeMapper.setIdClassMapping(mappings);
        return typeMapper;
    }
}


