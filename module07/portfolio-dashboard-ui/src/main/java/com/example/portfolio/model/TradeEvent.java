package com.example.portfolio.model;

public class TradeEvent {

    private String tradeId;
    private String stockSymbol;
    private int quantity;
    private String eventType;

    public TradeEvent() {
    }

    public TradeEvent(String tradeId, String stockSymbol, int quantity, String eventType) {
        this.tradeId = tradeId;
        this.stockSymbol = stockSymbol;
        this.quantity = quantity;
        this.eventType = eventType;
    }

    public String getTradeId() {
        return tradeId;
    }

    public String getStockSymbol() {
        return stockSymbol;
    }

    public int getQuantity() {
        return quantity;
    }

    public String getEventType() {
        return eventType;
    }
}