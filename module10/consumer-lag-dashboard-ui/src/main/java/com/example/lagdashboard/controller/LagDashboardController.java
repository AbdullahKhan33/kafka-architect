package com.example.lagdashboard.controller;

import com.example.lagdashboard.consumer.SlowOrderConsumer;
import com.example.lagdashboard.model.LagSummary;
import com.example.lagdashboard.producer.OrderEventProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
public class LagDashboardController {

    private final OrderEventProducer orderEventProducer;
    private final SlowOrderConsumer slowOrderConsumer;

    public LagDashboardController(
            OrderEventProducer orderEventProducer,
            SlowOrderConsumer slowOrderConsumer) {

        this.orderEventProducer = orderEventProducer;
        this.slowOrderConsumer = slowOrderConsumer;
    }

    @GetMapping("/")
    public String showDashboard(Model model) {

        addDashboardData(model);

        return "lag-dashboard";
    }

    @PostMapping("/orders/single")
    public String publishSingleOrder(
            @RequestParam String customerName,
            @RequestParam String productName,
            @RequestParam double amount) {

        orderEventProducer.publishSingleOrder(
                customerName,
                productName,
                amount
        );

        return "redirect:/";
    }

    @PostMapping("/orders/bulk")
    public String publishBulkOrders(
            @RequestParam int count) {

        orderEventProducer.publishBulkOrders(count);

        return "redirect:/";
    }

    @PostMapping("/clear")
    public String clearDashboard() {

        orderEventProducer.resetProducedCount();
        slowOrderConsumer.clearConsumerView();

        return "redirect:/";
    }

    private void addDashboardData(Model model) {

        int producedCount =
                orderEventProducer.getProducedCount();

        int consumedCount =
                slowOrderConsumer.getConsumedCount();

        int estimatedLag =
                producedCount - consumedCount;

        if (estimatedLag < 0) {
            estimatedLag = 0;
        }

        LagSummary lagSummary =
                new LagSummary(
                        producedCount,
                        consumedCount,
                        estimatedLag
                );

        model.addAttribute(
                "lagSummary",
                lagSummary
        );

        model.addAttribute(
                "consumedLogs",
                slowOrderConsumer.getConsumedLogs()
        );
    }
}