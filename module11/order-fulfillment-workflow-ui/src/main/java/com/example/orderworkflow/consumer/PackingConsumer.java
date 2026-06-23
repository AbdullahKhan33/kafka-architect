package com.example.orderworkflow.consumer;

import com.example.orderworkflow.model.OrderWorkflowEvent;
import com.example.orderworkflow.producer.WorkflowEventProducer;
import com.example.orderworkflow.service.OrderTraceService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PackingConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final WorkflowEventProducer workflowEventProducer;
    private final OrderTraceService orderTraceService;

    public PackingConsumer(
            WorkflowEventProducer workflowEventProducer,
            OrderTraceService orderTraceService) {

        this.workflowEventProducer = workflowEventProducer;
        this.orderTraceService = orderTraceService;
    }

    @KafkaListener(
            topics = "payment-events-topic",
            groupId = "packing-service-group"
    )
    public void consume(ConsumerRecord<String, String> record)
            throws Exception {

        OrderWorkflowEvent event =
                objectMapper.readValue(record.value(), OrderWorkflowEvent.class);

        orderTraceService.addTrace(
                event,
                record,
                "PackingConsumer"
        );

        Thread.sleep(700);

        workflowEventProducer.publishOrderPacked(event);

        System.out.println("PackingConsumer processed PAYMENT_RECEIVED:");
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("--------------------------------");
    }
}