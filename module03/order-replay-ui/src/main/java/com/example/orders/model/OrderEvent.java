package com.example.orders.model;

public class OrderEvent {

    private String orderId;
    private String customerName;
    private double amount;
    private String eventType;

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String customerName, double amount, String eventType) {
        this.orderId = orderId;
        this.customerName = customerName;
        this.amount = amount;
        this.eventType = eventType;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public double getAmount() {
        return amount;
    }

    public String getEventType() {
        return eventType;
    }
}