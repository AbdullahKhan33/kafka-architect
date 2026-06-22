package com.example.portfolio.producer;

import com.example.portfolio.model.TradeEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TradeProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TradeProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendTrade(TradeEvent tradeEvent) {
        try {
            String eventJson = objectMapper.writeValueAsString(tradeEvent);

            kafkaTemplate.send(
                    "portfolio-trades-topic",
                    tradeEvent.getStockSymbol(),
                    eventJson
            );

            System.out.println("Trade Event Published:");
            System.out.println("Key: " + tradeEvent.getStockSymbol());
            System.out.println("Value: " + eventJson);

        } catch (Exception e) {
            throw new RuntimeException("Error publishing trade event", e);
        }
    }
}