package com.example.orders.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class BillingConsumer {

    @KafkaListener(topics = "orders-topic", groupId = "billing-service")
    public void consume(String event) {
        System.out.println("Billing Service Processed: " + event);
    }
}