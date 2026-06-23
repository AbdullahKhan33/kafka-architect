package com.example.orderworkflow.model;

public class OrderWorkflowEvent {

    private String eventId;
    private String orderId;
    private String correlationId;
    private String customerName;
    private String productName;
    private double amount;
    private String eventType;
    private String status;
    private String sourceService;
    private String eventTime;

    public OrderWorkflowEvent() {
    }

    public OrderWorkflowEvent(
            String eventId,
            String orderId,
            String correlationId,
            String customerName,
            String productName,
            double amount,
            String eventType,
            String status,
            String sourceService,
            String eventTime) {

        this.eventId = eventId;
        this.orderId = orderId;
        this.correlationId = correlationId;
        this.customerName = customerName;
        this.productName = productName;
        this.amount = amount;
        this.eventType = eventType;
        this.status = status;
        this.sourceService = sourceService;
        this.eventTime = eventTime;
    }

    public String getEventId() {
        return eventId;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCorrelationId() {
        return correlationId;
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

    public String getStatus() {
        return status;
    }

    public String getSourceService() {
        return sourceService;
    }

    public String getEventTime() {
        return eventTime;
    }
}