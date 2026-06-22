package com.example.trading.controller;

import com.example.trading.model.TradeEvent;
import com.example.trading.producer.TradeProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Controller
public class TradeController {

    private final TradeProducer tradeProducer;

    public TradeController(TradeProducer tradeProducer) {
        this.tradeProducer = tradeProducer;
    }

    @GetMapping("/")
    public String showTradePage() {
        return "trade";
    }

    @PostMapping("/trades")
    public String placeTrade(
            @RequestParam String stockSymbol,
            @RequestParam String tradeType,
            @RequestParam int quantity,
            @RequestParam double price,
            Model model) {

        String tradeId = "TRD-" + UUID.randomUUID().toString().substring(0, 5);

        TradeEvent tradeEvent = new TradeEvent(
                tradeId,
                stockSymbol.toUpperCase(),
                tradeType,
                quantity,
                price,
                "TRADE_PLACED"
        );

        tradeProducer.sendTradeEvent(tradeEvent);

        model.addAttribute("message", "Trade event published successfully.");
        model.addAttribute("tradeId", tradeId);
        model.addAttribute("stockSymbol", stockSymbol.toUpperCase());

        return "trade";
    }
}