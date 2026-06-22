package com.example.orders.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ReplayConsumer {

    @KafkaListener(topics = "orders-topic", groupId = "analytics-service")
    public void consume(String event) {
        System.out.println("Replay Service Read History: " + event);
    }
}