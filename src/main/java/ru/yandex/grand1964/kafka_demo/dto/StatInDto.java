package ru.yandex.grand1964.kafka_demo.dto;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class StatInDto {
    private String app;
    private String uri;
    private String ip;
    private String timestamp;

    @Override
    public String toString() {
        return String.format("{\"app\":%s,\"uri\":%s,\"ip\":%s,\"timestamp:\":%s}",app,uri,ip,timestamp);
    }
}
