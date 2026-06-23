package com.example.inventory.model;

public class StockAlertEvent {

    private String alertId;
    private String productId;
    private String productName;
    private int currentStock;
    private int reorderThreshold;
    private String warehouseLocation;
    private String alertMessage;
    private String alertLevel;
    private String eventTime;

    public StockAlertEvent() {
    }

    public StockAlertEvent(
            String alertId,
            String productId,
            String productName,
            int currentStock,
            int reorderThreshold,
            String warehouseLocation,
            String alertMessage,
            String alertLevel,
            String eventTime) {

        this.alertId = alertId;
        this.productId = productId;
        this.productName = productName;
        this.currentStock = currentStock;
        this.reorderThreshold = reorderThreshold;
        this.warehouseLocation = warehouseLocation;
        this.alertMessage = alertMessage;
        this.alertLevel = alertLevel;
        this.eventTime = eventTime;
    }

    public String getAlertId() {
        return alertId;
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

    public String getAlertMessage() {
        return alertMessage;
    }

    public String getAlertLevel() {
        return alertLevel;
    }

    public String getEventTime() {
        return eventTime;
    }
}