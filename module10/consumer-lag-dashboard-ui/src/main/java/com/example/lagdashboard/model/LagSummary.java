package com.example.lagdashboard.model;

public class LagSummary {

    private int producedCount;
    private int consumedCount;
    private int estimatedLag;

    public LagSummary() {
    }

    public LagSummary(
            int producedCount,
            int consumedCount,
            int estimatedLag) {

        this.producedCount = producedCount;
        this.consumedCount = consumedCount;
        this.estimatedLag = estimatedLag;
    }

    public int getProducedCount() {
        return producedCount;
    }

    public int getConsumedCount() {
        return consumedCount;
    }

    public int getEstimatedLag() {
        return estimatedLag;
    }
}