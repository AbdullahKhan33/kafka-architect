package com.example.ecommerce.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic ordersTopic() {
        return new NewTopic("ecommerce-orders-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic inventoryTopic() {
        return new NewTopic("ecommerce-inventory-topic", 3, (short) 1);
    }
}