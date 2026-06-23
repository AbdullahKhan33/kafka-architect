package com.example.lagdashboard.model;

public class ConsumedOrderLog {

    private String orderId;
    private String customerName;
    private String productName;
    private double amount;
    private String topic;
    private int partition;
    private long offset;
    private String consumedTime;

    public ConsumedOrderLog() {
    }

    public ConsumedOrderLog(
            String orderId,
            String customerName,
            String productName,
            double amount,
            String topic,
            int partition,
            long offset,
            String consumedTime) {

        this.orderId = orderId;
        this.customerName = customerName;
        this.productName = productName;
        this.amount = amount;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.consumedTime = consumedTime;
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

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public String getConsumedTime() {
        return consumedTime;
    }
}