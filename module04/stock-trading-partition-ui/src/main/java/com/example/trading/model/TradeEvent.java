package com.example.trading.model;

public class TradeEvent {

    private String tradeId;
    private String stockSymbol;
    private String tradeType;
    private int quantity;
    private double price;
    private String eventType;

    public TradeEvent() {
    }

    public TradeEvent(String tradeId, String stockSymbol, String tradeType, int quantity, double price, String eventType) {
        this.tradeId = tradeId;
        this.stockSymbol = stockSymbol;
        this.tradeType = tradeType;
        this.quantity = quantity;
        this.price = price;
        this.eventType = eventType;
    }

    public String getTradeId() {
        return tradeId;
    }

    public String getStockSymbol() {
        return stockSymbol;
    }

    public String getTradeType() {
        return tradeType;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getPrice() {
        return price;
    }

    public String getEventType() {
        return eventType;
    }
}