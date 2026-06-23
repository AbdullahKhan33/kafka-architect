package com.example.inventory.model;

public class InventoryView {

    private String productId;
    private String productName;
    private int currentStock;
    private int reorderThreshold;
    private String warehouseLocation;
    private String stockStatus;
    private String lastUpdatedTime;

    public InventoryView() {
    }

    public InventoryView(
            String productId,
            String productName,
            int currentStock,
            int reorderThreshold,
            String warehouseLocation,
            String stockStatus,
            String lastUpdatedTime) {

        this.productId = productId;
        this.productName = productName;
        this.currentStock = currentStock;
        this.reorderThreshold = reorderThreshold;
        this.warehouseLocation = warehouseLocation;
        this.stockStatus = stockStatus;
        this.lastUpdatedTime = lastUpdatedTime;
    }

    public String getProductId() {
        return productId;
    }

    public String getProductName() {
        return productName;
    }

    public int getCurrentStock() {
        return currentStock;
    }

    public int getReorderThreshold() {
        return reorderThreshold;
    }

    public String getWarehouseLocation() {
        return warehouseLocation;
    }

    public String getStockStatus() {
        return stockStatus;
    }

    public String getLastUpdatedTime() {
        return lastUpdatedTime;
    }
}