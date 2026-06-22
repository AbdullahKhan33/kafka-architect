package com.example.ecommerce.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryConsumer {

    @KafkaListener(topics = "ecommerce-inventory-topic", groupId = "inventory-consumer-group")
    public void consume(ConsumerRecord<String, String> record) {

        System.out.println("Inventory Consumer Received COMMITTED Event");
        System.out.println("Topic: " + record.topic());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("--------------------------------------");
    }
}