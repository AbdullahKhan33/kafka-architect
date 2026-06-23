package com.example.delivery.model;

public class DeliveryStatusView {

    private String orderId;
    private String customerName;
    private String productName;
    private String currentStatus;
    private String lastUpdatedTime;

    public DeliveryStatusView() {
    }

    public DeliveryStatusView(
            String orderId,
            String customerName,
            String productName,
            String currentStatus,
            String lastUpdatedTime) {

        this.orderId = orderId;
        this.customerName = customerName;
        this.productName = productName;
        this.currentStatus = currentStatus;
        this.lastUpdatedTime = lastUpdatedTime;
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

    public String getCurrentStatus() {
        return currentStatus;
    }

    public String getLastUpdatedTime() {
        return lastUpdatedTime;
    }
}