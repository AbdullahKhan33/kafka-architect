package com.example.inventory.consumer;

import com.example.inventory.model.EventActivityLog;
import com.example.inventory.model.StockAlertEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
public class StockAlertConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final List<StockAlertEvent> stockAlerts =
            Collections.synchronizedList(new ArrayList<>());

    private final List<EventActivityLog> alertActivityLogs =
            Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(
            topics = "stock-alert-topic",
            groupId = "stock-alert-dashboard-group"
    )
    public void consumeStockAlert(ConsumerRecord<String, String> record)
            throws Exception {

        StockAlertEvent stockAlertEvent =
                objectMapper.readValue(record.value(), StockAlertEvent.class);

        stockAlerts.add(0, stockAlertEvent);

        if (stockAlerts.size() > 50) {
            stockAlerts.remove(stockAlerts.size() - 1);
        }

        EventActivityLog activityLog =
                new EventActivityLog(
                        "StockAlertConsumer",
                        "STOCK_ALERT_CREATED",
                        stockAlertEvent.getProductId(),
                        stockAlertEvent.getProductName(),
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        stockAlertEvent.getAlertMessage()
                );

        alertActivityLogs.add(0, activityLog);

        if (alertActivityLogs.size() > 50) {
            alertActivityLogs.remove(alertActivityLogs.size() - 1);
        }

        System.out.println("Stock Alert Consumed:");
        System.out.println("Product ID: " + stockAlertEvent.getProductId());
        System.out.println("Alert Level: " + stockAlertEvent.getAlertLevel());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("--------------------------------");
    }

    public List<StockAlertEvent> getStockAlerts() {
        return stockAlerts;
    }

    public List<EventActivityLog> getAlertActivityLogs() {
        return alertActivityLogs;
    }

    public void clearAlerts() {
        stockAlerts.clear();
        alertActivityLogs.clear();
    }
}