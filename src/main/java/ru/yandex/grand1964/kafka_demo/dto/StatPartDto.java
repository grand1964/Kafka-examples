package ru.yandex.grand1964.kafka_demo.dto;

import lombok.*;

import java.util.Objects;

@Getter
@Setter
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StatPartDto s)) {
            return false;
        }
        return (s.uri.equals(s.getUri()) && s.ip.equals(s.getIp()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, ip);
    }
}
