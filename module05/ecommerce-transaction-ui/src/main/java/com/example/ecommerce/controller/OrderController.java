package com.example.ecommerce.controller;

import com.example.ecommerce.service.TransactionalOrderService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
public class OrderController {

    private final TransactionalOrderService transactionalOrderService;

    public OrderController(TransactionalOrderService transactionalOrderService) {
        this.transactionalOrderService = transactionalOrderService;
    }

    @GetMapping("/")
    public String showOrderPage() {
        return "order";
    }

    @PostMapping("/place-order")
    public String placeOrder(
            @RequestParam String customerName,
            @RequestParam String productName,
            @RequestParam int quantity,
            @RequestParam double price,
            @RequestParam(required = false) String simulateFailure,
            Model model) {

        boolean shouldFail = simulateFailure != null;

        try {
            String orderId = transactionalOrderService.placeOrder(
                    customerName,
                    productName,
                    quantity,
                    price,
                    shouldFail
            );

            model.addAttribute("successMessage", "Transaction committed successfully.");
            model.addAttribute("orderId", orderId);

        } catch (Exception e) {

            e.printStackTrace();

            model.addAttribute(
                    "errorMessage",
                    "Transaction aborted. No partial event should be visible.");

            model.addAttribute(
                    "errorDetails",
                    e.getMessage());
        }

        return "order";
    }
}