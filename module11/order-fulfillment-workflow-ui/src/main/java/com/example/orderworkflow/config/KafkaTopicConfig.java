package com.example.orderworkflow.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderEventsTopic() {
        return new NewTopic("order-events-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic paymentEventsTopic() {
        return new NewTopic("payment-events-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic packingEventsTopic() {
        return new NewTopic("packing-events-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic shippingEventsTopic() {
        return new NewTopic("shipping-events-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic deliveryEventsTopic() {
        return new NewTopic("delivery-events-topic", 3, (short) 1);
    }
}