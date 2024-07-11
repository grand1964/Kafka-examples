package ru.yandex.grand1964.kafka_demo.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StatPartDto {
    private String uri;
    private String ip;
    private String timestamp;

    public StatPartDto(StatInDto inDto) {
        super();
        uri = inDto.getUri();
        ip = inDto.getIp();
        timestamp = inDto.getTimestamp();
    }
}
