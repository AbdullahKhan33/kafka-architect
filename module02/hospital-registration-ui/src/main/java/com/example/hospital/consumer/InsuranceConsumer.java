package com.example.hospital.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InsuranceConsumer {

    @KafkaListener(topics = "patient-events", groupId = "insurance-service")
    public void consume(String event) {
        System.out.println("Insurance Service Received: " + event);
    }
}