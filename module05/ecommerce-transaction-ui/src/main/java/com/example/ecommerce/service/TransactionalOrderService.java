package com.example.ecommerce.service;

import com.example.ecommerce.model.InventoryEvent;
import com.example.ecommerce.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class TransactionalOrderService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TransactionalOrderService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Transactional
    public String placeOrder(
            String customerName,
            String productName,
            int quantity,
            double price,
            boolean simulateFailure) {

        try {
            String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 5);

            OrderEvent orderEvent = new OrderEvent(
                    orderId,
                    customerName,
                    productName,
                    quantity,
                    price,
                    "ORDER_PLACED"
            );

            InventoryEvent inventoryEvent = new InventoryEvent(
                    "INV-" + UUID.randomUUID().toString().substring(0, 5),
                    orderId,
                    productName,
                    quantity,
                    "INVENTORY_RESERVED"
            );

            String orderJson = objectMapper.writeValueAsString(orderEvent);
            String inventoryJson = objectMapper.writeValueAsString(inventoryEvent);

            kafkaTemplate.send(
                    "ecommerce-orders-topic",
                    orderId,
                    orderJson
            );

            System.out.println("Order event sent inside transaction:");
            System.out.println(orderJson);

            if (simulateFailure) {
                throw new RuntimeException("Simulated failure after order event. Transaction should rollback.");
            }

            kafkaTemplate.send(
                    "ecommerce-inventory-topic",
                    orderId,
                    inventoryJson
            );

            System.out.println("Inventory event sent inside transaction:");
            System.out.println(inventoryJson);

            return orderId;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}