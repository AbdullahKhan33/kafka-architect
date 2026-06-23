package com.example.lagdashboard.consumer;

import com.example.lagdashboard.model.ConsumedOrderLog;
import com.example.lagdashboard.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SlowOrderConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final List<ConsumedOrderLog> consumedLogs =
            Collections.synchronizedList(new ArrayList<>());

    private final AtomicInteger consumedCount = new AtomicInteger(0);

    @KafkaListener(
            topics = "orders-lag-topic",
            groupId = "slow-reporting-group"
    )
    public void consume(ConsumerRecord<String, String> record)
            throws Exception {

        Thread.sleep(2000);

        OrderEvent orderEvent =
                objectMapper.readValue(record.value(), OrderEvent.class);

        ConsumedOrderLog consumedOrderLog =
                new ConsumedOrderLog(
                        orderEvent.getOrderId(),
                        orderEvent.getCustomerName(),
                        orderEvent.getProductName(),
                        orderEvent.getAmount(),
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        LocalDateTime.now().toString()
                );

        consumedLogs.add(0, consumedOrderLog);

        if (consumedLogs.size() > 50) {
            consumedLogs.remove(consumedLogs.size() - 1);
        }

        consumedCount.incrementAndGet();

        System.out.println("Slow Consumer Processed Order:");
        System.out.println("Order ID: " + orderEvent.getOrderId());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("Consumed Count: " + consumedCount.get());
        System.out.println("--------------------------------");
    }

    public List<ConsumedOrderLog> getConsumedLogs() {
        return consumedLogs;
    }

    public int getConsumedCount() {
        return consumedCount.get();
    }

    public void clearConsumerView() {
        consumedLogs.clear();
        consumedCount.set(0);
    }
}