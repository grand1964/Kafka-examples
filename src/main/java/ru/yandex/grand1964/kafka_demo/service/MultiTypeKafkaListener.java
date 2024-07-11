package ru.yandex.grand1964.kafka_demo.service;

import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.yandex.grand1964.kafka_demo.dto.StatInDto;
import ru.yandex.grand1964.kafka_demo.dto.StatPartDto;

//@Component
//@KafkaListener(topics = "msg")
public class MultiTypeKafkaListener {

    /*//@KafkaHandler
    @KafkaListener(topics = "msg")
    public void handleFullStat(StatInDto statInDto) {
        System.out.println("Full stat received: " + statInDto);
    }

    //@KafkaHandler
    @KafkaListener(topics = "ewm-main-service")
    public void handlePartStat(StatPartDto statPartDto) {
        System.out.println("Partial stat received: " + statPartDto);
    }

    @KafkaHandler(isDefault = true)
    public void unknown(Object object) {
        System.out.println("Unkown type received: " + object);
    }*/
}
