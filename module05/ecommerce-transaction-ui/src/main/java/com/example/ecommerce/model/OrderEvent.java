package com.example.ecommerce.model;

public class OrderEvent {

    private String orderId;
    private String customerName;
    private String productName;
    private int quantity;
    private double price;
    private String eventType;

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String customerName, String productName, int quantity, double price, String eventType) {
        this.orderId = orderId;
        this.customerName = customerName;
        this.productName = productName;
        this.quantity = quantity;
        this.price = price;
        this.eventType = eventType;
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

    public int getQuantity() {
        return quantity;
    }

    public double getPrice() {
        return price;
    }

    public String getEventType() {
        return eventType;
    }
}