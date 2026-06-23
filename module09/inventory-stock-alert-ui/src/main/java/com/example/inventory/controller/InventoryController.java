package com.example.inventory.controller;

import com.example.inventory.consumer.InventoryEventConsumer;
import com.example.inventory.consumer.StockAlertConsumer;
import com.example.inventory.producer.InventoryEventProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
public class InventoryController {

    private final InventoryEventProducer inventoryEventProducer;
    private final InventoryEventConsumer inventoryEventConsumer;
    private final StockAlertConsumer stockAlertConsumer;

    public InventoryController(
            InventoryEventProducer inventoryEventProducer,
            InventoryEventConsumer inventoryEventConsumer,
            StockAlertConsumer stockAlertConsumer) {

        this.inventoryEventProducer = inventoryEventProducer;
        this.inventoryEventConsumer = inventoryEventConsumer;
        this.stockAlertConsumer = stockAlertConsumer;
    }

    @GetMapping("/")
    public String showPage(Model model) {
        addDashboardData(model);
        return "inventory";
    }

    @PostMapping("/inventory-events")
    public String publishInventoryEvent(
            @RequestParam String productId,
            @RequestParam String productName,
            @RequestParam int currentStock,
            @RequestParam int reorderThreshold,
            @RequestParam String warehouseLocation,
            Model model) {

        inventoryEventProducer.publishInventoryUpdate(
                productId,
                productName,
                currentStock,
                reorderThreshold,
                warehouseLocation
        );

        waitForConsumers();

        model.addAttribute(
                "message",
                "Inventory event published to Kafka."
        );

        model.addAttribute(
                "productId",
                productId
        );

        addDashboardData(model);

        return "inventory";
    }

    @PostMapping("/clear")
    public String clearDashboard() {

        inventoryEventConsumer.clearInventoryView();
        stockAlertConsumer.clearAlerts();

        return "redirect:/";
    }

    private void addDashboardData(Model model) {

        model.addAttribute(
                "inventory",
                inventoryEventConsumer.getInventoryViewMap()
        );

        model.addAttribute(
                "alerts",
                stockAlertConsumer.getStockAlerts()
        );

        model.addAttribute(
                "inventoryLogs",
                inventoryEventConsumer.getActivityLogs()
        );

        model.addAttribute(
                "alertLogs",
                stockAlertConsumer.getAlertActivityLogs()
        );
    }

    private void waitForConsumers() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}