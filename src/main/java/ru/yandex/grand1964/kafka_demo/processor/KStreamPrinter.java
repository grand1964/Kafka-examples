package ru.yandex.grand1964.kafka_demo.processor;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import ru.yandex.grand1964.kafka_demo.dto.StatOutDto;

public class KStreamPrinter
        implements ProcessorSupplier<String, StatOutDto, String, StatOutDto> {
    private final String name;
    public KStreamPrinter(String name) {
        this.name = name;
    }

    @Override
    public Processor<String, StatOutDto, String, StatOutDto> get() {
        return new PrintingProcessor(this.name);
    }


    private static class PrintingProcessor
            extends ContextualProcessor<String, StatOutDto, String, StatOutDto> {
        private final String name;


        PrintingProcessor(String name) {
            this.name = name;
        }

        @Override
        public void process(Record<String, StatOutDto> record) {
            System.out.printf("[%s] Key [%s] Value[%s]%n", name,
                    record.key(), record.value());
            this.context().forward(record);
        }
    }

}
