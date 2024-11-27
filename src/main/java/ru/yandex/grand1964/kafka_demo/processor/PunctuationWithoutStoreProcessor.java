package ru.yandex.grand1964.kafka_demo.processor;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import ru.yandex.grand1964.kafka_demo.dto.StatOutDto;
import ru.yandex.grand1964.kafka_demo.punctuator.StatPunctuator;

import java.time.Duration;

public class PunctuationWithoutStoreProcessor extends
        ContextualProcessor<String, StatOutDto, String, StatOutDto> {
        //ContextualProcessor<Windowed<String>, StatOutDto, String, StatOutDto> {
    private static final String STORE_NAME_SLIDING = "slidingStore";
    private final Duration punctuationDuration;
    private ProcessorContext<String, StatOutDto> context;
    private KeyValueStore<String, StatOutDto> store;

    public PunctuationWithoutStoreProcessor(Duration punctuationDuration) {
        this.punctuationDuration = punctuationDuration;
    }

    @Override
    public void init(ProcessorContext<String, StatOutDto> context) {
        super.init(context);
        this.context = context;
        store = context.getStateStore(STORE_NAME_SLIDING);
        StatPunctuator punctuator = new StatPunctuator(context, store);
        //TODO ВЕРНУТЬ !!!!!!!!!!!!!!!!
        context.schedule(punctuationDuration, PunctuationType.STREAM_TIME, punctuator);
        //context.schedule(punctuationDuration, PunctuationType.WALL_CLOCK_TIME, punctuator);
    }

    @Override
    public void process(Record<String, StatOutDto> record) {
    //public void process(Record<Windowed<String>, StatOutDto> record) {

        //TODO ???????????????????
        System.out.println("Пришли в process пунктуации!!!");
        System.out.println("Запись, key=" + record.key() + ", timestamp=" + record.timestamp());
        //if (!record.key().contains("mocked")) {
            context.forward(record);
        //}



        /*if (record.key().contains("mocked")) {
            context.forward(record);
        }*/
        /*try ( KeyValueIterator<?, ?> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<?, ?> item = iterator.next();
                System.out.println("ЗАПИСЬ ИЗ ХРАНИЛИЩА: key=" + item.key);
            }
        }*/

        /*
        //анализируем запись
        //String key = record.key(); //ключ (URI)
        String key = record.key().key(); //ключ (URI)
        StatOutDto v = record.value(); //входное значение

        //TODO ПРИМОЧКА - ТОЛЬКО ДЛЯ ТЕСТИРОВАНИЯ
        if (key.contains("mocked")) {
        //if ("".equals(key)) {
            System.out.println("ПОЛУЧЕНА ФИКТИВНАЯ ЗАПИСЬ: " + record.timestamp());
            context.forward(record.withKey(key));
        } else {
            //пишем данные в хранилище (уже без окна в ключе)
            store.put(key, v);
            //дальше запись не посылаем (это делает пунктуатор)
            //TODO УБРАТЬ!!!!!
            System.out.println("Window: " + record.key().window() + ", Key: " + key);
            //TODO ??????????????????????
            //context.forward(record.withKey(key));
        }*/
    }
}
