package com.example.orderworkflow.service;

import com.example.orderworkflow.model.OrderStatusView;
import com.example.orderworkflow.model.OrderTraceEvent;
import com.example.orderworkflow.model.OrderWorkflowEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class OrderTraceService {

    private final Map<String, List<OrderTraceEvent>> traceStore =
            new ConcurrentHashMap<>();

    private final Map<String, OrderStatusView> orderStatusMap =
            new ConcurrentHashMap<>();

    private final List<OrderTraceEvent> allEvents =
            Collections.synchronizedList(new ArrayList<>());

    public void addTrace(
            OrderWorkflowEvent event,
            ConsumerRecord<String, String> record,
            String serviceName) {

        OrderTraceEvent traceEvent =
                new OrderTraceEvent(
                        event.getOrderId(),
                        event.getCorrelationId(),
                        event.getEventType(),
                        event.getStatus(),
                        serviceName,
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        event.getEventTime()
                );

        traceStore
                .computeIfAbsent(
                        event.getOrderId(),
                        key -> Collections.synchronizedList(new ArrayList<>())
                )
                .add(traceEvent);

        allEvents.add(0, traceEvent);

        if (allEvents.size() > 100) {
            allEvents.remove(allEvents.size() - 1);
        }

        OrderStatusView statusView =
                new OrderStatusView(
                        event.getOrderId(),
                        event.getCorrelationId(),
                        event.getCustomerName(),
                        event.getProductName(),
                        event.getAmount(),
                        event.getStatus(),
                        serviceName,
                        event.getEventTime()
                );

        orderStatusMap.put(event.getOrderId(), statusView);
    }

    public List<OrderTraceEvent> getTraceByOrderId(String orderId) {

        List<OrderTraceEvent> events =
                traceStore.getOrDefault(
                        orderId,
                        Collections.emptyList()
                );

        List<OrderTraceEvent> sortedEvents =
                new ArrayList<>(events);

        sortedEvents.sort(
                Comparator.comparing(OrderTraceEvent::getEventTime)
        );

        return sortedEvents;
    }

    public Map<String, OrderStatusView> getOrderStatusMap() {
        return orderStatusMap;
    }

    public List<OrderTraceEvent> getAllEvents() {
        return allEvents;
    }

    public void clear() {
        traceStore.clear();
        orderStatusMap.clear();
        allEvents.clear();
    }
}