package com.example.ecommerce.model;

public class InventoryEvent {

    private String inventoryEventId;
    private String orderId;
    private String productName;
    private int quantity;
    private String eventType;

    public InventoryEvent() {
    }

    public InventoryEvent(String inventoryEventId, String orderId, String productName, int quantity, String eventType) {
        this.inventoryEventId = inventoryEventId;
        this.orderId = orderId;
        this.productName = productName;
        this.quantity = quantity;
        this.eventType = eventType;
    }

    public String getInventoryEventId() {
        return inventoryEventId;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getProductName() {
        return productName;
    }

    public int getQuantity() {
        return quantity;
    }

    public String getEventType() {
        return eventType;
    }
}