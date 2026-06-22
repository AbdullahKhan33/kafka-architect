package com.example.portfolio.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic tradesTopic() {
        return new NewTopic("portfolio-trades-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic portfolioSummaryTopic() {
        return new NewTopic("portfolio-summary-topic", 3, (short) 1);
    }
}