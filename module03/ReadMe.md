
# UI 2: Order Replay Portal

## Goal

Build a Spring Boot UI where:

```text
User places orders
        ↓
Spring Boot publishes events to Kafka
        ↓
Billing Consumer processes orders
        ↓
Replay Consumer can read full order history using a different group
```

This teaches:

```text
Offsets
Consumer Groups
Replay
Durability
Different group = full copy
Same group = continue from last offset
```

---

# Final Flow

```text
Browser UI
   ↓
OrderController
   ↓
OrderProducer
   ↓
orders-topic
   ↓
BillingConsumer
   ↓
ReplayConsumer
```

---

# Step 1: Create New Spring Boot Project

Project name:

```text
order-replay-ui
```

Dependencies:

```text
Spring Web
Spring for Apache Kafka
Thymeleaf
Lombok
```

---

# Step 2: Project Structure

```text
src/main/java/com/example/orders/

├── OrderReplayUiApplication.java
├── controller/
│   └── OrderController.java
├── model/
│   └── OrderEvent.java
├── producer/
│   └── OrderProducer.java
├── consumer/
│   ├── BillingConsumer.java
│   └── ReplayConsumer.java
└── config/
    └── KafkaTopicConfig.java

src/main/resources/
├── templates/
│   └── order.html
└── application.properties
```

---

# Step 3: application.properties

File:

```text
src/main/resources/application.properties
```

```properties
spring.application.name=order-replay-ui

server.port=8082

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.consumer.auto-offset-reset=earliest

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

---

#  Pom.xml

```
<dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
</dependency>
```

---

# Step 4: Main Class

File:

```text
OrderReplayUiApplication.java
```

```java
package com.example.orders;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrderReplayUiApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderReplayUiApplication.class, args);
    }

}
```

---

# Step 5: KafkaTopicConfig.java

Package:

```text
com.example.orders.config
```

```java
package com.example.orders.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic ordersTopic() {
        return new NewTopic("orders-topic", 3, (short) 1);
    }
}
```

---

# Step 6: OrderEvent.java

Package:

```text
com.example.orders.model
```

```java
package com.example.orders.model;

public class OrderEvent {

    private String orderId;
    private String customerName;
    private double amount;
    private String eventType;

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String customerName, double amount, String eventType) {
        this.orderId = orderId;
        this.customerName = customerName;
        this.amount = amount;
        this.eventType = eventType;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public double getAmount() {
        return amount;
    }

    public String getEventType() {
        return eventType;
    }
}
```

---

# Step 7: OrderProducer.java

Package:

```text
com.example.orders.producer
```

```java
package com.example.orders.producer;

import com.example.orders.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OrderProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderEvent(OrderEvent orderEvent) {
        try {
            String eventJson = objectMapper.writeValueAsString(orderEvent);

            kafkaTemplate.send(
                    "orders-topic",
                    orderEvent.getOrderId(),
                    eventJson
            );

            System.out.println("Published Order Event: " + eventJson);

        } catch (Exception e) {
            throw new RuntimeException("Error publishing order event", e);
        }
    }
}
```

---

# Step 8: OrderController.java

Package:

```text
com.example.orders.controller
```

```java
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
```

---

# Step 9: BillingConsumer.java

Package:

```text
com.example.orders.consumer
```

```java
package com.example.orders.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class BillingConsumer {

    @KafkaListener(topics = "orders-topic", groupId = "billing-service")
    public void consume(String event) {
        System.out.println("Billing Service Processed: " + event);
    }
}
```

---

# Step 10: ReplayConsumer.java

Package:

```text
com.example.orders.consumer
```

```java
package com.example.orders.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ReplayConsumer {

    @KafkaListener(topics = "orders-topic", groupId = "replay-service")
    public void consume(String event) {
        System.out.println("Replay Service Read History: " + event);
    }
}
```

---

# Step 11: order.html

File:

```text
src/main/resources/templates/order.html
```

```html
<!DOCTYPE html>
<html>
<head>
    <title>Order Replay Portal</title>
    <style>
        body {
            font-family: Arial;
            margin: 40px;
            background-color: #f4f6f8;
        }

        .container {
            display: flex;
            gap: 30px;
        }

        .card {
            width: 430px;
            background: white;
            padding: 25px;
            border-radius: 10px;
        }

        input, button {
            width: 100%;
            padding: 10px;
            margin-top: 12px;
        }

        button {
            background: #2e7d32;
            color: white;
            border: none;
            cursor: pointer;
        }

        .success {
            margin-top: 20px;
            color: green;
            font-weight: bold;
        }

        .info {
            background: #eef3ff;
            padding: 15px;
            border-radius: 8px;
            line-height: 1.6;
        }
    </style>
</head>
<body>

<h2>Order Replay Portal</h2>

<div class="container">

    <div class="card">
        <h3>Place Order</h3>

        <form method="post" action="/orders">
            <input type="text" name="customerName" placeholder="Customer Name" required>
            <input type="number" step="0.01" name="amount" placeholder="Order Amount" required>

            <button type="submit">Place Order</button>
        </form>

        <div class="success" th:if="${message}">
            <p th:text="${message}"></p>
            <p>Order ID: <span th:text="${orderId}"></span></p>
        </div>
    </div>

    <div class="card">
        <h3>Kafka Concepts</h3>

        <div class="info">
            <p><b>Producer:</b> This UI publishes order events.</p>
            <p><b>Topic:</b> orders-topic stores events.</p>
            <p><b>Billing Consumer:</b> Processes orders.</p>
            <p><b>Replay Consumer:</b> Reads order history using another group.</p>
            <p><b>Key Idea:</b> Different consumer groups can replay the same topic independently.</p>
        </div>
    </div>

</div>

</body>
</html>
```

---

# Step 12: Start Kafka

```bash
docker start zookeeper
sleep 15
docker start kafka
sleep 15
docker ps
```

Verify:

```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

# Step 13: Run Application

Run:

```text
OrderReplayUiApplication
```

Open:

```text
http://localhost:8082
```

Submit:

```text
Customer Name: Abdullah
Amount: 5000
```

Submit more:

```text
Customer Name: Ahmed
Amount: 2500
```

---

# Expected Console Output

```text
Published Order Event: {"orderId":"ORD-a1234","customerName":"Abdullah","amount":5000.0,"eventType":"ORDER_PLACED"}

Billing Service Processed: {"orderId":"ORD-a1234","customerName":"Abdullah","amount":5000.0,"eventType":"ORDER_PLACED"}

Replay Service Read History: {"orderId":"ORD-a1234","customerName":"Abdullah","amount":5000.0,"eventType":"ORDER_PLACED"}
```

---

# Step 14: Explain the Demo

## Billing Consumer

```java
groupId = "billing-service"
```

This group tracks its own offset.

## Replay Consumer

```java
groupId = "replay-service"
```

This is a separate group.

So it gets its own independent offset.

```text
orders-topic
     ↓
billing-service offset
     ↓
replay-service offset
```

---

# Step 15: Classroom Experiment

Stop the application.

Change ReplayConsumer group id:

```java
groupId = "replay-service-v2"
```

Run again.

Now `replay-service-v2` is a brand-new consumer group, so it can read old messages from the beginning because:

```properties
spring.kafka.consumer.auto-offset-reset=earliest
```

Expected:

```text
Replay Service Read History: old orders also appear
```

---



# This is exactly what happened:

```text
                         orders-topic
        ------------------------------------------------
        | Offset 0 | Offset 1 | Offset 2 | Offset 3 |
        | ORD1001  | ORD1002  | ORD1003  | ORD1004  |
        ------------------------------------------------
              ↑
              Kafka keeps these messages
              even after Billing consumes them
```

```text
                 Consumer Group 1
                 billing-service

        Reads:
        Offset 0 → ORD1001
        Offset 1 → ORD1002
        Offset 2 → ORD1003

        Bookmark stored:
        billing-service offset = 3
```

```text
                 Consumer Group 2
                 replay-service-v2

        Brand new group
        No previous bookmark

        Because:
        auto-offset-reset = earliest

        Kafka says:
        "Start from Offset 0"

        Reads:
        Offset 0 → ORD1001
        Offset 1 → ORD1002
        Offset 2 → ORD1003
        Offset 3 → ORD1004
```

# Final Picture

```text
                  Order UI
                     |
                     v
              Order Producer
                     |
                     v
              orders-topic
   ------------------------------------------------
   | ORD1001 | ORD1002 | ORD1003 | ORD1004 |
   ------------------------------------------------
        |                         |
        |                         |
        v                         v

 billing-service             replay-service-v2
 already consumed            new consumer group
 starts from latest          starts from beginning
 using its offset            because no old offset
```

# Key Learning

```text
Kafka topic stores the events.

Consumer group stores the reading position.

New consumer group = new reading position.

So replay becomes possible.
```

# How many partitions

 **it created 3 partitions**, because in `KafkaTopicConfig` we had:

```java
@Bean
public NewTopic ordersTopic() {
    return new NewTopic("orders-topic", 3, (short) 1);
}
```

So internally:

```text
orders-topic

----------------------
| P0 | P1 | P2 |
----------------------
```

However, there is an important subtlety.

### Your two consumers:

```text
billing-service
replay-service
```

are **different consumer groups**.

Therefore both groups receive **all messages**.

Conceptually:

```text
                 orders-topic
         -------------------------
         | P0 | P1 | P2 |
         -------------------------
              /           \
             /             \
 billing-service       replay-service
 (group 1)             (group 2)

Both read all partitions
Both maintain separate offsets
```

---

### Inside billing-service

You currently have only one consumer instance:

```java
@KafkaListener(
    topics = "orders-topic",
    groupId = "billing-service"
)
```

Therefore one consumer owns all partitions:

```text
billing-service

Consumer 1

P0
P1
P2
```

No parallelism yet.
