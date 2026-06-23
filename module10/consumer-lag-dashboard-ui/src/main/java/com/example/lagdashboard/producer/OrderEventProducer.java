package com.example.lagdashboard.producer;

import com.example.lagdashboard.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class OrderEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AtomicInteger producedCount = new AtomicInteger(0);

    public OrderEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishSingleOrder(
            String customerName,
            String productName,
            double amount) {

        publishOrder(customerName, productName, amount);
    }

    public void publishBulkOrders(int count) {

        for (int i = 1; i <= count; i++) {
            publishOrder(
                    "Customer-" + i,
                    "Product-" + i,
                    1000 + i
            );
        }
    }

    private void publishOrder(
            String customerName,
            String productName,
            double amount) {

        try {
            String orderId =
                    "ORD-" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();

            OrderEvent orderEvent =
                    new OrderEvent(
                            orderId,
                            customerName,
                            productName,
                            amount,
                            "ORDER_CREATED",
                            LocalDateTime.now().toString()
                    );

            String eventJson =
                    objectMapper.writeValueAsString(orderEvent);

            kafkaTemplate.send(
                    "orders-lag-topic",
                    orderId,
                    eventJson
            );

            producedCount.incrementAndGet();

            System.out.println("Order Event Published:");
            System.out.println("Key: " + orderId);
            System.out.println("Value: " + eventJson);
            System.out.println("--------------------------------");

        } catch (Exception e) {
            throw new RuntimeException("Error publishing order event", e);
        }
    }

    public int getProducedCount() {
        return producedCount.get();
    }

    public void resetProducedCount() {
        producedCount.set(0);
    }
}