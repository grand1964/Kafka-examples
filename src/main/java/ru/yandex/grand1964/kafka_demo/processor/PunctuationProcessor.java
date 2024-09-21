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

public class PunctuationProcessor extends
        ContextualProcessor<Windowed<String>, StatOutDto, String, StatOutDto> {
    private static final String STORE_NAME_SLIDING = "slidingStore";
    private final Duration punctuationDuration;
    //private ProcessorContext<String, StatOutDto> context;
    private KeyValueStore<String, StatOutDto> store;

    public PunctuationProcessor(Duration punctuationDuration) {
        this.punctuationDuration = punctuationDuration;
    }

    @Override
    public void init(ProcessorContext<String, StatOutDto> context) {
        //this.context = context;
        store = context.getStateStore(STORE_NAME_SLIDING);
        StatPunctuator punctuator = new StatPunctuator(context);
        context.schedule(punctuationDuration, PunctuationType.STREAM_TIME, punctuator);
    }

    @Override
    public void process(Record<Windowed<String>, StatOutDto> record) {
        //анализируем запись
        String key = record.key().key(); //ключ (URI)
        StatOutDto v = record.value(); //входное значение
        //пишем данные в хранилище (уже без окна в ключе)
        store.put(key, v);
        //дальше запись не посылаем (это делает пунктуатор)
        //TODO УБРАТЬ!!!!!
        System.out.println("Window: " + record.key().window());
    }
}
