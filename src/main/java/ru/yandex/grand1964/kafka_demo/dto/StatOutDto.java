package ru.yandex.grand1964.kafka_demo.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StatOutDto {
    private String app;
    private String uri;
    private Long hits;
}
