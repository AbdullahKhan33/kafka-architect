package com.example.orderworkflow.model;

public class OrderStatusView {

    private String orderId;
    private String correlationId;
    private String customerName;
    private String productName;
    private double amount;
    private String currentStatus;
    private String lastUpdatedBy;
    private String lastUpdatedTime;

    public OrderStatusView() {
    }

    public OrderStatusView(
            String orderId,
            String correlationId,
            String customerName,
            String productName,
            double amount,
            String currentStatus,
            String lastUpdatedBy,
            String lastUpdatedTime) {

        this.orderId = orderId;
        this.correlationId = correlationId;
        this.customerName = customerName;
        this.productName = productName;
        this.amount = amount;
        this.currentStatus = currentStatus;
        this.lastUpdatedBy = lastUpdatedBy;
        this.lastUpdatedTime = lastUpdatedTime;
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

    public String getCurrentStatus() {
        return currentStatus;
    }

    public String getLastUpdatedBy() {
        return lastUpdatedBy;
    }

    public String getLastUpdatedTime() {
        return lastUpdatedTime;
    }
}