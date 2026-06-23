package com.example.orderworkflow.consumer;

import com.example.orderworkflow.model.OrderWorkflowEvent;
import com.example.orderworkflow.service.OrderTraceService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class FinalDeliveryConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OrderTraceService orderTraceService;

    public FinalDeliveryConsumer(OrderTraceService orderTraceService) {
        this.orderTraceService = orderTraceService;
    }

    @KafkaListener(
            topics = "delivery-events-topic",
            groupId = "final-dashboard-group"
    )
    public void consume(ConsumerRecord<String, String> record)
            throws Exception {

        OrderWorkflowEvent event =
                objectMapper.readValue(record.value(), OrderWorkflowEvent.class);

        orderTraceService.addTrace(
                event,
                record,
                "FinalDeliveryConsumer"
        );

        System.out.println("FinalDeliveryConsumer recorded DELIVERED:");
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("--------------------------------");
    }
}