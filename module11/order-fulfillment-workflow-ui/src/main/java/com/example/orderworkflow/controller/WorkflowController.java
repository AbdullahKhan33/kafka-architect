package com.example.orderworkflow.controller;

import com.example.orderworkflow.model.OrderStatusView;
import com.example.orderworkflow.model.OrderTraceEvent;
import com.example.orderworkflow.producer.WorkflowEventProducer;
import com.example.orderworkflow.service.OrderTraceService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Controller
public class WorkflowController {

    private final WorkflowEventProducer workflowEventProducer;
    private final OrderTraceService orderTraceService;

    public WorkflowController(
            WorkflowEventProducer workflowEventProducer,
            OrderTraceService orderTraceService) {

        this.workflowEventProducer = workflowEventProducer;
        this.orderTraceService = orderTraceService;
    }

    @GetMapping("/")
    public String showPage(Model model) {

        model.addAttribute("traceMode", false);
        model.addAttribute("searchedOrderId", "");
        model.addAttribute("selectedTrace", List.of());
        model.addAttribute("selectedTraceCount", 0);

        model.addAttribute(
                "orders",
                orderTraceService.getOrderStatusMap()
        );

        model.addAttribute(
                "allEvents",
                orderTraceService.getAllEvents()
        );

        return "workflow";
    }

    @PostMapping("/orders")
    public String placeOrder(
            @RequestParam String customerName,
            @RequestParam String productName,
            @RequestParam double amount) {

        workflowEventProducer.placeOrder(
                customerName,
                productName,
                amount
        );

        return "redirect:/";
    }

    @GetMapping("/trace")
    public String traceOrder(
            @RequestParam String orderId,
            Model model) {

        String cleanedOrderId =
                orderId.trim().toUpperCase();

        System.out.println("TRACE BUTTON CLICKED");
        System.out.println("Trace requested for Order ID: " + cleanedOrderId);

        List<OrderTraceEvent> selectedTrace =
                orderTraceService.getTraceByOrderId(cleanedOrderId);

        System.out.println("Trace records found: " + selectedTrace.size());

        Map<String, OrderStatusView> filteredOrders =
                new LinkedHashMap<>();

        OrderStatusView selectedOrder =
                orderTraceService
                        .getOrderStatusMap()
                        .get(cleanedOrderId);

        if (selectedOrder != null) {
            filteredOrders.put(cleanedOrderId, selectedOrder);
        }

        model.addAttribute("traceMode", true);
        model.addAttribute("searchedOrderId", cleanedOrderId);
        model.addAttribute("selectedTrace", selectedTrace);
        model.addAttribute("selectedTraceCount", selectedTrace.size());

        model.addAttribute(
                "orders",
                filteredOrders
        );

        model.addAttribute(
                "allEvents",
                selectedTrace
        );

        return "workflow";
    }

    @PostMapping("/clear")
    public String clear() {

        orderTraceService.clear();

        return "redirect:/";
    }
}