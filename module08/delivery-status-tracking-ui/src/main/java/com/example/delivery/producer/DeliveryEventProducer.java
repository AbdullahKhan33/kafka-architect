package com.example.delivery.producer;

import com.example.delivery.model.DeliveryEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class DeliveryEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DeliveryEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String createOrder(
            String customerName,
            String productName) {

        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();

        publishDeliveryEvent(
                orderId,
                customerName,
                productName,
                "ORDER_PLACED",
                "ORDER_PLACED"
        );

        return orderId;
    }

    public void updateDeliveryStatus(
            String orderId,
            String customerName,
            String productName,
            String deliveryStatus) {

        publishDeliveryEvent(
                orderId,
                customerName,
                productName,
                deliveryStatus,
                deliveryStatus
        );
    }

    private void publishDeliveryEvent(
            String orderId,
            String customerName,
            String productName,
            String deliveryStatus,
            String eventType) {

        try {
            String eventId = "EVT-" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();

            DeliveryEvent deliveryEvent = new DeliveryEvent(
                    eventId,
                    orderId,
                    customerName,
                    productName,
                    deliveryStatus,
                    eventType,
                    LocalDateTime.now().toString()
            );

            String eventJson = objectMapper.writeValueAsString(deliveryEvent);

            kafkaTemplate.send(
                    "delivery-status-topic",
                    orderId,
                    eventJson
            );

            System.out.println("Delivery Event Published:");
            System.out.println("Key: " + orderId);
            System.out.println("Value: " + eventJson);
            System.out.println("--------------------------------");

        } catch (Exception e) {
            throw new RuntimeException("Error publishing delivery event", e);
        }
    }
}