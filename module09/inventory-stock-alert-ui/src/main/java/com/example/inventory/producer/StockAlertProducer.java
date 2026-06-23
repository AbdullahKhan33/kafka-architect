package com.example.inventory.producer;

import com.example.inventory.model.StockAlertEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class StockAlertProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public StockAlertProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishStockAlert(
            String productId,
            String productName,
            int currentStock,
            int reorderThreshold,
            String warehouseLocation) {

        try {
            String alertId =
                    "ALT-" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();

            String alertLevel;

            if (currentStock == 0) {
                alertLevel = "CRITICAL";
            } else {
                alertLevel = "LOW";
            }

            String alertMessage =
                    "Stock is below or equal to reorder threshold. Reorder required.";

            StockAlertEvent stockAlertEvent =
                    new StockAlertEvent(
                            alertId,
                            productId,
                            productName,
                            currentStock,
                            reorderThreshold,
                            warehouseLocation,
                            alertMessage,
                            alertLevel,
                            LocalDateTime.now().toString()
                    );

            String alertJson =
                    objectMapper.writeValueAsString(stockAlertEvent);

            kafkaTemplate.send(
                    "stock-alert-topic",
                    productId,
                    alertJson
            );

            System.out.println("Stock Alert Published:");
            System.out.println("Key: " + productId);
            System.out.println("Value: " + alertJson);
            System.out.println("--------------------------------");

        } catch (Exception e) {
            throw new RuntimeException("Error publishing stock alert event", e);
        }
    }
}