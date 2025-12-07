package com.media.activityconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class ActivityConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ActivityConsumerApplication.class, args);
    }
}