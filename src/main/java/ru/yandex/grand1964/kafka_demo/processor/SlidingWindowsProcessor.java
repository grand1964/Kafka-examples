package ru.yandex.grand1964.kafka_demo.processor;

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import ru.yandex.grand1964.kafka_demo.dto.StatOutDto;

public class SlidingWindowsProcessor extends
        ContextualProcessor<Windowed<String>, StatOutDto, String, StatOutDto> {
    private static final String STORE_NAME_SLIDING = "slidingStore";
    private ProcessorContext<String, StatOutDto> context;
    private KeyValueStore<String, StatOutDto> store;

    @Override
    public void init(ProcessorContext<String, StatOutDto> context) {
        this.context = context;
        store = context.getStateStore(STORE_NAME_SLIDING);
    }

    @Override
    public void process(Record<Windowed<String>, StatOutDto> record) {
        //анализируем запись
        String key = record.key().key(); //ключ (URI)
        StatOutDto v = record.value(); //входное значение
        store.put(key, v);
        System.out.println("Window: " + record.key().window());
        context.forward(record
                .withKey(key)
                .withValue(v));
    }
}
