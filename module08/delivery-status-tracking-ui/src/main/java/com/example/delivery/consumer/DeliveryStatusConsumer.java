package com.example.delivery.consumer;

import com.example.delivery.model.DeliveryEvent;
import com.example.delivery.model.DeliveryEventLog;
import com.example.delivery.model.DeliveryStatusView;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DeliveryStatusConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Map<String, DeliveryStatusView> currentStatusMap =
            new ConcurrentHashMap<>();

    private final List<DeliveryEventLog> eventLogs =
            Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(
            topics = "delivery-status-topic",
            groupId = "delivery-dashboard-group"
    )
    public void consume(ConsumerRecord<String, String> record) throws Exception {

        DeliveryEvent deliveryEvent =
                objectMapper.readValue(record.value(), DeliveryEvent.class);

        DeliveryStatusView statusView =
                new DeliveryStatusView(
                        deliveryEvent.getOrderId(),
                        deliveryEvent.getCustomerName(),
                        deliveryEvent.getProductName(),
                        deliveryEvent.getDeliveryStatus(),
                        deliveryEvent.getEventTime()
                );

        currentStatusMap.put(
                deliveryEvent.getOrderId(),
                statusView
        );

        DeliveryEventLog eventLog =
                new DeliveryEventLog(
                        deliveryEvent.getOrderId(),
                        deliveryEvent.getEventType(),
                        deliveryEvent.getDeliveryStatus(),
                        deliveryEvent.getCustomerName(),
                        deliveryEvent.getProductName(),
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        deliveryEvent.getEventTime()
                );

        eventLogs.add(0, eventLog);

        if (eventLogs.size() > 50) {
            eventLogs.remove(eventLogs.size() - 1);
        }

        System.out.println("Delivery Dashboard Updated:");
        System.out.println("Order ID: " + deliveryEvent.getOrderId());
        System.out.println("Status: " + deliveryEvent.getDeliveryStatus());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("--------------------------------");
    }

    public Map<String, DeliveryStatusView> getCurrentStatusMap() {
        return currentStatusMap;
    }

    public List<DeliveryEventLog> getEventLogs() {
        return eventLogs;
    }

    public void clearLogs() {
        currentStatusMap.clear();
        eventLogs.clear();
    }
}