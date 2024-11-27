package ru.yandex.grand1964.kafka_demo.punctuator;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import ru.yandex.grand1964.kafka_demo.dto.StatOutDto;

import java.time.LocalDateTime;

public class StatPunctuator implements Punctuator {
    //private static final String STORE_NAME_SLIDING = "slidingStore";
    private final ProcessorContext<String, StatOutDto> context;
    private final KeyValueStore<String, StatOutDto> store;

    public StatPunctuator(ProcessorContext<String, StatOutDto> context, KeyValueStore<String, StatOutDto> store) {
        this.context = context;
        this.store = store;
        //store = context.getStateStore(STORE_NAME_SLIDING);
    }

    @Override
    public void punctuate(long l) {
        try (KeyValueIterator<String, StatOutDto> statIterator = store.all()) {
            System.out.println("PUNCTUATION TIME: " + l);
            System.out.println("Системное время: " + LocalDateTime.now());
            System.out.println("**********************************");
            while (statIterator.hasNext()) {
                KeyValue<String, StatOutDto> keyValue = statIterator.next();
                //TODO УБРАТЬ!!!!!
                String key = keyValue.key;
                //String key = keyValue.key + "_PROCESSED";
                StatOutDto value = keyValue.value;
                Record<String, StatOutDto> record = new Record<>(key, value, l);
                //Record<String, StatOutDto> record = new Record<>(keyValue.key, keyValue.value, l);
                context.forward(record);
            }
        }
    }
}
