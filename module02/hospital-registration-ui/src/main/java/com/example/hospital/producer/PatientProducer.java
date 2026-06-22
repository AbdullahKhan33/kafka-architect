package com.example.hospital.producer;

import com.example.hospital.model.PatientEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PatientProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PatientProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPatientEvent(PatientEvent patientEvent) {
        try {
            String eventJson = objectMapper.writeValueAsString(patientEvent);
            kafkaTemplate.send("patient-events", patientEvent.getPatientId(), eventJson);
            System.out.println("Published Event: " + eventJson);
        } catch (Exception e) {
            throw new RuntimeException("Error publishing patient event", e);
        }
    }
}