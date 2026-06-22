package com.example.orders.controller;

import com.example.orders.model.OrderEvent;
import com.example.orders.producer.OrderProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Controller
public class OrderController {

    private final OrderProducer orderProducer;

    public OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    @GetMapping("/")
    public String showOrderPage() {
        return "order";
    }

    @PostMapping("/orders")
    public String placeOrder(
            @RequestParam String customerName,
            @RequestParam double amount,
            Model model) {

        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 5);

        OrderEvent orderEvent = new OrderEvent(
                orderId,
                customerName,
                amount,
                "ORDER_PLACED"
        );

        orderProducer.sendOrderEvent(orderEvent);

        model.addAttribute("message", "Order placed and event published successfully.");
        model.addAttribute("orderId", orderId);

        return "order";
    }
}