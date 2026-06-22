package com.example.orders.producer;

import com.example.orders.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OrderProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderEvent(OrderEvent orderEvent) {
        try {
            String eventJson = objectMapper.writeValueAsString(orderEvent);

            kafkaTemplate.send(
                    "orders-topic",
                    orderEvent.getOrderId(),
                    eventJson
            );

            System.out.println("Published Order Event: " + eventJson);

        } catch (Exception e) {
            throw new RuntimeException("Error publishing order event", e);
        }
    }
}