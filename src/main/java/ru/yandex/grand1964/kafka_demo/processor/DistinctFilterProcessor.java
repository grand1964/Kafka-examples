package ru.yandex.grand1964.kafka_demo.processor;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import ru.yandex.grand1964.kafka_demo.dto.StatOutDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

import java.util.Collections;
import java.util.HashSet;

public class DistinctFilterProcessor extends
        ContextualProcessor<String, StatPartDto, String, StatOutDto> {
    private static final String STATE_STORE_DISTINCT = "distinctStore";
    private static final String TOPIC_NAME = "common";

    private ProcessorContext<String, StatOutDto> context;
    private KeyValueStore<String, HashSet<String>> store;

    @Override
    public void init(ProcessorContext<String, StatOutDto> context) {
        this.context = context;
        store = context.getStateStore(STATE_STORE_DISTINCT);
    }

    @Override
    public void process(Record<String, StatPartDto> record) {
        //анализируем запись
        String key = record.key(); //ключ (URI)
        StatPartDto v = record.value(); //входное значение
        HashSet<String> ips = store.get(key); //все ip с заданным URI
        //генерируем выходное значение с count=1
        StatOutDto vNew = new StatOutDto(TOPIC_NAME, key, 1L);
        if (ips == null) { //такого URI еще не было
            //пишем в хранилище по заданному URI одноэлементное множество
            store.put(key, new HashSet<>(Collections.singleton(v.getIp())));
            //отправляем преобразованную запись дальше по цепочке
            context.forward(record.withValue(vNew));
        } else if (!ips.contains(v.getIp())) { //URI уже был, но не с таким ip
            ips.add(v.getIp()); //добавляем новый ip в множество
            store.put(key, ips); //пишем новое множество в хранилище
            context.forward(record.withValue(vNew)); //отправляем запись дальше
        }
        //если пара (URI, ip) уже есть - запись дальше не отправляется
    }
}
