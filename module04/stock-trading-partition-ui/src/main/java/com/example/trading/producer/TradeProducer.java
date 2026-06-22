package com.example.trading.producer;

import com.example.trading.model.TradeEvent;
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

    public void sendTradeEvent(TradeEvent tradeEvent) {
        try {
            String eventJson = objectMapper.writeValueAsString(tradeEvent);

            kafkaTemplate.send(
                    "trades-topic",
                    tradeEvent.getStockSymbol(),
                    eventJson
            );

            System.out.println("Published Trade Event");
            System.out.println("Key: " + tradeEvent.getStockSymbol());
            System.out.println("Value: " + eventJson);

        } catch (Exception e) {
            throw new RuntimeException("Error publishing trade event", e);
        }
    }
}