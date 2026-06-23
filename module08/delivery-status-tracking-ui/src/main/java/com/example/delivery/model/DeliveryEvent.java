package com.example.delivery.model;

public class DeliveryEvent {

    private String eventId;
    private String orderId;
    private String customerName;
    private String productName;
    private String deliveryStatus;
    private String eventType;
    private String eventTime;

    public DeliveryEvent() {
    }

    public DeliveryEvent(
            String eventId,
            String orderId,
            String customerName,
            String productName,
            String deliveryStatus,
            String eventType,
            String eventTime) {

        this.eventId = eventId;
        this.orderId = orderId;
        this.customerName = customerName;
        this.productName = productName;
        this.deliveryStatus = deliveryStatus;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getDeliveryStatus() {
        return deliveryStatus;
    }

    public void setDeliveryStatus(String deliveryStatus) {
        this.deliveryStatus = deliveryStatus;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }
}