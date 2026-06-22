package com.example.hospital.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class DoctorConsumer {

    @KafkaListener(topics = "patient-events", groupId = "doctor-service")
    public void consume(String event) {
        System.out.println("Doctor Service Received: " + event);
    }
}