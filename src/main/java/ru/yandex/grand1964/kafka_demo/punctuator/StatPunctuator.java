package ru.yandex.grand1964.kafka_demo.punctuator;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import ru.yandex.grand1964.kafka_demo.dto.StatOutDto;

public class StatPunctuator implements Punctuator {
    private static final String STORE_NAME_SLIDING = "slidingStore";
    private ProcessorContext<String, StatOutDto> context;
    private KeyValueStore<String, StatOutDto> store;

    public StatPunctuator(ProcessorContext<String, StatOutDto> context) {
        this.context = context;
        store = context.getStateStore(STORE_NAME_SLIDING);
    }
    @Override
    public void punctuate(long l) {
        try (KeyValueIterator<String, StatOutDto> performanceIterator = store.all()) {
            while (performanceIterator.hasNext()) {
                KeyValue<String, StatOutDto> keyValue = performanceIterator.next();
                String key = keyValue.key;
                StatOutDto value = keyValue.value;
                Record<String, StatOutDto> record =
                        new Record<>(key, value, l);
                context.forward(record);
            }
        }
    }
}
