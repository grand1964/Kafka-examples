package ru.yandex.grand1964.kafka_demo.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class StatRecord {
    private String uri;
    private String ip;
    private Long timestamp;
}
