package com.example.orderworkflow.consumer;

import com.example.orderworkflow.model.OrderWorkflowEvent;
import com.example.orderworkflow.producer.WorkflowEventProducer;
import com.example.orderworkflow.service.OrderTraceService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ShippingConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final WorkflowEventProducer workflowEventProducer;
    private final OrderTraceService orderTraceService;

    public ShippingConsumer(
            WorkflowEventProducer workflowEventProducer,
            OrderTraceService orderTraceService) {

        this.workflowEventProducer = workflowEventProducer;
        this.orderTraceService = orderTraceService;
    }

    @KafkaListener(
            topics = "packing-events-topic",
            groupId = "shipping-service-group"
    )
    public void consume(ConsumerRecord<String, String> record)
            throws Exception {

        OrderWorkflowEvent event =
                objectMapper.readValue(record.value(), OrderWorkflowEvent.class);

        orderTraceService.addTrace(
                event,
                record,
                "ShippingConsumer"
        );

        Thread.sleep(700);

        workflowEventProducer.publishOrderShipped(event);

        System.out.println("ShippingConsumer processed ORDER_PACKED:");
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("--------------------------------");
    }
}