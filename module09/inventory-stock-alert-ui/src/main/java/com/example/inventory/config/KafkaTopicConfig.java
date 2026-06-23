package com.example.inventory.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic inventoryEventsTopic() {
        return new NewTopic("inventory-events-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic stockAlertTopic() {
        return new NewTopic("stock-alert-topic", 3, (short) 1);
    }
}