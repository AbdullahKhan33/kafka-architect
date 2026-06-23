package com.example.inventory.model;

public class EventActivityLog {

    private String eventSource;
    private String eventType;
    private String productId;
    private String productName;
    private String topic;
    private int partition;
    private long offset;
    private String message;

    public EventActivityLog() {
    }

    public EventActivityLog(
            String eventSource,
            String eventType,
            String productId,
            String productName,
            String topic,
            int partition,
            long offset,
            String message) {

        this.eventSource = eventSource;
        this.eventType = eventType;
        this.productId = productId;
        this.productName = productName;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.message = message;
    }

    public String getEventSource() {
        return eventSource;
    }

    public String getEventType() {
        return eventType;
    }

    public String getProductId() {
        return productId;
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

    public String getMessage() {
        return message;
    }
}