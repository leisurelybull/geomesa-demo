package org.sisyphus.demo.kafka.test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class Time {
    public static void main(String[] args) {
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);
        System.out.println(LocalDateTime.parse("2015-01-01T11:56:43".replace("T"," "), dateFormat).toInstant(ZoneOffset.UTC));
    }

}
