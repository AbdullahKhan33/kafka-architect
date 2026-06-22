package com.example.portfolio.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DashboardConsumer {

    private final Map<String, String> portfolioSummary = new ConcurrentHashMap<>();

    @KafkaListener(topics = "portfolio-summary-topic", groupId = "portfolio-dashboard-group")
    public void consume(ConsumerRecord<String, String> record) {

        portfolioSummary.put(record.key(), record.value());

        System.out.println("Dashboard Updated:");
        System.out.println(record.key() + " = " + record.value());
        System.out.println("--------------------------------");
    }

    public Map<String, String> getPortfolioSummary() {
        return portfolioSummary;
    }
}