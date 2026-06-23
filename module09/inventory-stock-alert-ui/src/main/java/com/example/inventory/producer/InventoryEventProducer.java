package com.example.inventory.producer;

import com.example.inventory.model.InventoryEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class InventoryEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public InventoryEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishInventoryUpdate(
            String productId,
            String productName,
            int currentStock,
            int reorderThreshold,
            String warehouseLocation) {

        try {
            String eventId =
                    "INV-" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();

            InventoryEvent inventoryEvent =
                    new InventoryEvent(
                            eventId,
                            productId,
                            productName,
                            currentStock,
                            reorderThreshold,
                            warehouseLocation,
                            "INVENTORY_UPDATED",
                            LocalDateTime.now().toString()
                    );

            String eventJson =
                    objectMapper.writeValueAsString(inventoryEvent);

            kafkaTemplate.send(
                    "inventory-events-topic",
                    productId,
                    eventJson
            );

            System.out.println("Inventory Event Published:");
            System.out.println("Key: " + productId);
            System.out.println("Value: " + eventJson);
            System.out.println("--------------------------------");

        } catch (Exception e) {
            throw new RuntimeException("Error publishing inventory event", e);
        }
    }
}