package com.example.orderworkflow.model;

public class OrderTraceEvent {

    private String orderId;
    private String correlationId;
    private String eventType;
    private String status;
    private String sourceService;
    private String topic;
    private int partition;
    private long offset;
    private String eventTime;

    public OrderTraceEvent() {
    }

    public OrderTraceEvent(
            String orderId,
            String correlationId,
            String eventType,
            String status,
            String sourceService,
            String topic,
            int partition,
            long offset,
            String eventTime) {

        this.orderId = orderId;
        this.correlationId = correlationId;
        this.eventType = eventType;
        this.status = status;
        this.sourceService = sourceService;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.eventTime = eventTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCorrelationId() {
        return correlationId;
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