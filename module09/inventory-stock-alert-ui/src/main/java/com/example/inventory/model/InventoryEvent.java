package com.example.inventory.model;

public class InventoryEvent {

    private String eventId;
    private String productId;
    private String productName;
    private int currentStock;
    private int reorderThreshold;
    private String warehouseLocation;
    private String eventType;
    private String eventTime;

    public InventoryEvent() {
    }

    public InventoryEvent(
            String eventId,
            String productId,
            String productName,
            int currentStock,
            int reorderThreshold,
            String warehouseLocation,
            String eventType,
            String eventTime) {

        this.eventId = eventId;
        this.productId = productId;
        this.productName = productName;
        this.currentStock = currentStock;
        this.reorderThreshold = reorderThreshold;
        this.warehouseLocation = warehouseLocation;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public String getEventId() {
        return eventId;
    }

    public String getProductId() {
        return productId;
    }

    public String getProductName() {
        return productName;
    }

    public int getCurrentStock() {
        return currentStock;
    }

    public int getReorderThreshold() {
        return reorderThreshold;
    }

    public String getWarehouseLocation() {
        return warehouseLocation;
    }

    public String getEventType() {
        return eventType;
    }

    public String getEventTime() {
        return eventTime;
    }
}