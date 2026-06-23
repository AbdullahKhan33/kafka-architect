package com.example.delivery.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic deliveryStatusTopic() {
        return new NewTopic("delivery-status-topic", 3, (short) 1);
    }
}