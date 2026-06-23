package com.example.inventory.consumer;

import com.example.inventory.model.EventActivityLog;
import com.example.inventory.model.InventoryEvent;
import com.example.inventory.model.InventoryView;
import com.example.inventory.producer.StockAlertProducer;
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
public class InventoryEventConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final StockAlertProducer stockAlertProducer;

    private final Map<String, InventoryView> inventoryViewMap =
            new ConcurrentHashMap<>();

    private final List<EventActivityLog> activityLogs =
            Collections.synchronizedList(new ArrayList<>());

    public InventoryEventConsumer(StockAlertProducer stockAlertProducer) {
        this.stockAlertProducer = stockAlertProducer;
    }

    @KafkaListener(
            topics = "inventory-events-topic",
            groupId = "inventory-monitor-group"
    )
    public void consumeInventoryEvent(ConsumerRecord<String, String> record)
            throws Exception {

        InventoryEvent inventoryEvent =
                objectMapper.readValue(record.value(), InventoryEvent.class);

        String stockStatus;

        if (inventoryEvent.getCurrentStock()
                <= inventoryEvent.getReorderThreshold()) {

            stockStatus = "LOW_STOCK";

            stockAlertProducer.publishStockAlert(
                    inventoryEvent.getProductId(),
                    inventoryEvent.getProductName(),
                    inventoryEvent.getCurrentStock(),
                    inventoryEvent.getReorderThreshold(),
                    inventoryEvent.getWarehouseLocation()
            );

        } else {
            stockStatus = "STOCK_OK";
        }

        InventoryView inventoryView =
                new InventoryView(
                        inventoryEvent.getProductId(),
                        inventoryEvent.getProductName(),
                        inventoryEvent.getCurrentStock(),
                        inventoryEvent.getReorderThreshold(),
                        inventoryEvent.getWarehouseLocation(),
                        stockStatus,
                        inventoryEvent.getEventTime()
                );

        inventoryViewMap.put(
                inventoryEvent.getProductId(),
                inventoryView
        );

        String message =
                "Inventory event consumed. Stock status calculated as "
                        + stockStatus;

        EventActivityLog activityLog =
                new EventActivityLog(
                        "InventoryEventConsumer",
                        inventoryEvent.getEventType(),
                        inventoryEvent.getProductId(),
                        inventoryEvent.getProductName(),
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        message
                );

        activityLogs.add(0, activityLog);

        if (activityLogs.size() > 50) {
            activityLogs.remove(activityLogs.size() - 1);
        }

        System.out.println("Inventory Event Consumed:");
        System.out.println("Product ID: " + inventoryEvent.getProductId());
        System.out.println("Current Stock: " + inventoryEvent.getCurrentStock());
        System.out.println("Threshold: " + inventoryEvent.getReorderThreshold());
        System.out.println("Status: " + stockStatus);
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("--------------------------------");
    }

    public Map<String, InventoryView> getInventoryViewMap() {
        return inventoryViewMap;
    }

    public List<EventActivityLog> getActivityLogs() {
        return activityLogs;
    }

    public void clearInventoryView() {
        inventoryViewMap.clear();
        activityLogs.clear();
    }
}