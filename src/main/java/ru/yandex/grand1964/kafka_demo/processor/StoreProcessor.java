package ru.yandex.grand1964.kafka_demo.processor;

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import ru.yandex.grand1964.kafka_demo.dto.StatOutDto;
import ru.yandex.grand1964.kafka_demo.punctuator.StatPunctuator;

import java.time.Duration;

public class StoreProcessor extends
        ContextualProcessor<Windowed<String>, StatOutDto, String, StatOutDto> {
    private static final String STORE_NAME_SLIDING = "slidingStore";
    //private ProcessorContext<String, StatOutDto> context;
    private KeyValueStore<String, StatOutDto> store;

    @Override
    public void init(ProcessorContext<String, StatOutDto> context) {
        super.init(context);
        //this.context = context;
        store = context.getStateStore(STORE_NAME_SLIDING);
    }

    @Override
    public void process(Record<Windowed<String>, StatOutDto> record) {
        //анализируем запись
        String key = record.key().key(); //ключ (URI)
        StatOutDto v = record.value(); //входное значение
        if (!key.contains("mocked")) {
            //пишем данные в хранилище (уже без окна в ключе)
            store.put(key, v);
            System.out.println("Store record to storage: ");
            System.out.println("Window: " + record.key().window() + ", Key: " + key);
            //TODO !!!!!!!!!!!!!!!!!!!!!!
            //context().forward(record.withKey(key));
        } else {
            v.setHits(record.timestamp());
            context().forward(record
                    .withKey(key)
                    .withValue(v));
        }
    }
}
