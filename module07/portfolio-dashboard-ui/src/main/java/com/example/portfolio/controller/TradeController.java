package com.example.portfolio.controller;

import com.example.portfolio.consumer.DashboardConsumer;
import com.example.portfolio.model.TradeEvent;
import com.example.portfolio.producer.TradeProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class TradeController {

    private final TradeProducer tradeProducer;
    private final DashboardConsumer dashboardConsumer;

    public TradeController(
            TradeProducer tradeProducer,
            DashboardConsumer dashboardConsumer) {

        this.tradeProducer = tradeProducer;
        this.dashboardConsumer = dashboardConsumer;
    }

    @GetMapping("/")
    public String showPage(Model model) {
        model.addAttribute(
                "portfolio",
                dashboardConsumer.getPortfolioSummary()
        );

        return "trade";
    }

    @PostMapping("/trades")
    public String submitTrade(
            @RequestParam String stockSymbol,
            @RequestParam int quantity,
            Model model) {

        String tradeId =
                "TRD-" + UUID.randomUUID().toString().substring(0, 5);

        TradeEvent tradeEvent =
                new TradeEvent(
                        tradeId,
                        stockSymbol,
                        quantity,
                        "TRADE_SUBMITTED"
                );

        tradeProducer.sendTrade(tradeEvent);

        model.addAttribute("message", "Trade submitted to Kafka Streams.");
        model.addAttribute("tradeId", tradeId);

        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        model.addAttribute(
                "portfolio",
                dashboardConsumer.getPortfolioSummary()
        );

        return "trade";
    }

    @GetMapping("/portfolio")
    @ResponseBody
    public Map<String, String> getPortfolio() {
        return dashboardConsumer.getPortfolioSummary();
    }
}