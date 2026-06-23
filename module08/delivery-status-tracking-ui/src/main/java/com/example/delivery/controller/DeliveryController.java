package com.example.delivery.controller;

import com.example.delivery.consumer.DeliveryStatusConsumer;
import com.example.delivery.producer.DeliveryEventProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
public class DeliveryController {

    private final DeliveryEventProducer deliveryEventProducer;
    private final DeliveryStatusConsumer deliveryStatusConsumer;

    public DeliveryController(
            DeliveryEventProducer deliveryEventProducer,
            DeliveryStatusConsumer deliveryStatusConsumer) {

        this.deliveryEventProducer = deliveryEventProducer;
        this.deliveryStatusConsumer = deliveryStatusConsumer;
    }

    @GetMapping("/")
    public String showPage(Model model) {

        model.addAttribute(
                "orders",
                deliveryStatusConsumer.getCurrentStatusMap()
        );

        model.addAttribute(
                "logs",
                deliveryStatusConsumer.getEventLogs()
        );

        return "delivery";
    }

    @PostMapping("/orders")
    public String createOrder(
            @RequestParam String customerName,
            @RequestParam String productName,
            Model model) {

        String orderId =
                deliveryEventProducer.createOrder(
                        customerName,
                        productName
                );

        waitForConsumer();

        model.addAttribute("message", "Order created and ORDER_PLACED event published.");
        model.addAttribute("orderId", orderId);

        model.addAttribute(
                "orders",
                deliveryStatusConsumer.getCurrentStatusMap()
        );

        model.addAttribute(
                "logs",
                deliveryStatusConsumer.getEventLogs()
        );

        return "delivery";
    }

    @PostMapping("/delivery-status")
    public String updateDeliveryStatus(
            @RequestParam String orderId,
            @RequestParam String customerName,
            @RequestParam String productName,
            @RequestParam String deliveryStatus,
            Model model) {

        deliveryEventProducer.updateDeliveryStatus(
                orderId,
                customerName,
                productName,
                deliveryStatus
        );

        waitForConsumer();

        model.addAttribute("message", "Delivery status event published.");
        model.addAttribute("orderId", orderId);

        model.addAttribute(
                "orders",
                deliveryStatusConsumer.getCurrentStatusMap()
        );

        model.addAttribute(
                "logs",
                deliveryStatusConsumer.getEventLogs()
        );

        return "delivery";
    }

    @PostMapping("/clear")
    public String clearDashboard() {
        deliveryStatusConsumer.clearLogs();
        return "redirect:/";
    }

    private void waitForConsumer() {
        try {
            Thread.sleep(800);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}