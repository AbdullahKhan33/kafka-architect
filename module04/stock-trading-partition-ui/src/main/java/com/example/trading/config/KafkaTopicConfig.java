package com.example.trading.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic tradesTopic() {
        return new NewTopic("trades-topic", 3, (short) 1);
    }
}