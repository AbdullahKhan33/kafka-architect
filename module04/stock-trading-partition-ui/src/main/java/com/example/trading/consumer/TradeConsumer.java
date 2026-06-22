package com.example.trading.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class TradeConsumer {

    @KafkaListener(topics = "trades-topic", groupId = "trade-processing-group")
    public void consume(ConsumerRecord<String, String> record) {

        System.out.println("Trade Consumer Received Event");
        System.out.println("Topic: " + record.topic());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("----------------------------------");
    }
}