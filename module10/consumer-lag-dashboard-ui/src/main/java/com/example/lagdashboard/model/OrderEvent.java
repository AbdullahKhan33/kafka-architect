package com.example.lagdashboard.model;

public class OrderEvent {

    private String orderId;
    private String customerName;
    private String productName;
    private double amount;
    private String eventType;
    private String eventTime;

    public OrderEvent() {
    }

    public OrderEvent(
            String orderId,
            String customerName,
            String productName,
            double amount,
            String eventType,
            String eventTime) {

        this.orderId = orderId;
        this.customerName = customerName;
        this.productName = productName;
        this.amount = amount;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public String getProductName() {
        return productName;
    }

    public double getAmount() {
        return amount;
    }

    public String getEventType() {
        return eventType;
    }

    public String getEventTime() {
        return eventTime;
    }
}