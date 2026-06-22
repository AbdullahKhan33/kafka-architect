package com.example.hospital.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic patientEventsTopic() {
        return new NewTopic("patient-events", 3, (short) 1);
    }
}
