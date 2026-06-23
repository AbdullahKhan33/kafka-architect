package com.example.orderworkflow.producer;

import com.example.orderworkflow.model.OrderWorkflowEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class WorkflowEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public WorkflowEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String placeOrder(
            String customerName,
            String productName,
            double amount) {

        String orderId =
                "ORD-" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();

        String correlationId = orderId;

        publishEvent(
                "order-events-topic",
                orderId,
                correlationId,
                customerName,
                productName,
                amount,
                "ORDER_PLACED",
                "ORDER_PLACED",
                "OrderService"
        );

        return orderId;
    }

    public void publishPaymentReceived(OrderWorkflowEvent previousEvent) {

        publishEvent(
                "payment-events-topic",
                previousEvent.getOrderId(),
                previousEvent.getCorrelationId(),
                previousEvent.getCustomerName(),
                previousEvent.getProductName(),
                previousEvent.getAmount(),
                "PAYMENT_RECEIVED",
                "PAYMENT_RECEIVED",
                "PaymentService"
        );
    }

    public void publishOrderPacked(OrderWorkflowEvent previousEvent) {

        publishEvent(
                "packing-events-topic",
                previousEvent.getOrderId(),
                previousEvent.getCorrelationId(),
                previousEvent.getCustomerName(),
                previousEvent.getProductName(),
                previousEvent.getAmount(),
                "ORDER_PACKED",
                "ORDER_PACKED",
                "PackingService"
        );
    }

    public void publishOrderShipped(OrderWorkflowEvent previousEvent) {

        publishEvent(
                "shipping-events-topic",
                previousEvent.getOrderId(),
                previousEvent.getCorrelationId(),
                previousEvent.getCustomerName(),
                previousEvent.getProductName(),
                previousEvent.getAmount(),
                "ORDER_SHIPPED",
                "ORDER_SHIPPED",
                "ShippingService"
        );
    }

    public void publishDelivered(OrderWorkflowEvent previousEvent) {

        publishEvent(
                "delivery-events-topic",
                previousEvent.getOrderId(),
                previousEvent.getCorrelationId(),
                previousEvent.getCustomerName(),
                previousEvent.getProductName(),
                previousEvent.getAmount(),
                "DELIVERED",
                "DELIVERED",
                "DeliveryService"
        );
    }

    private void publishEvent(
            String topic,
            String orderId,
            String correlationId,
            String customerName,
            String productName,
            double amount,
            String eventType,
            String status,
            String sourceService) {

        try {
            String eventId =
                    "EVT-" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();

            OrderWorkflowEvent event =
                    new OrderWorkflowEvent(
                            eventId,
                            orderId,
                            correlationId,
                            customerName,
                            productName,
                            amount,
                            eventType,
                            status,
                            sourceService,
                            LocalDateTime.now().toString()
                    );

            String eventJson =
                    objectMapper.writeValueAsString(event);

            kafkaTemplate.send(
                    topic,
                    orderId,
                    eventJson
            );

            System.out.println("Workflow Event Published:");
            System.out.println("Topic: " + topic);
            System.out.println("Key: " + orderId);
            System.out.println("Event Type: " + eventType);
            System.out.println("Value: " + eventJson);
            System.out.println("--------------------------------");

        } catch (Exception e) {
            throw new RuntimeException("Error publishing workflow event", e);
        }
    }
}