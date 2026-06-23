package com.example.delivery.model;

public class DeliveryEventLog {

    private String orderId;
    private String eventType;
    private String deliveryStatus;
    private String customerName;
    private String productName;
    private String topic;
    private int partition;
    private long offset;
    private String eventTime;

    public DeliveryEventLog() {
    }

    public DeliveryEventLog(
            String orderId,
            String eventType,
            String deliveryStatus,
            String customerName,
            String productName,
            String topic,
            int partition,
            long offset,
            String eventTime) {

        this.orderId = orderId;
        this.eventType = eventType;
        this.deliveryStatus = deliveryStatus;
        this.customerName = customerName;
        this.productName = productName;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.eventTime = eventTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getDeliveryStatus() {
        return deliveryStatus;
    }

    public String getCustomerName() {
        return customerName;
    }

    public String getProductName() {
        return productName;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public String getEventTime() {
        return eventTime;
    }
}