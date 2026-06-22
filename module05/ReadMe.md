
# UI 4: E-Commerce Kafka Transaction Publisher

## Goal

Build a Spring Boot UI where one button publishes to **two Kafka topics inside one transaction**.

```text
Browser UI
   ↓
OrderController
   ↓
TransactionalOrderService
   ↓
BEGIN Kafka Transaction
   ↓
orders-topic
inventory-topic
   ↓
COMMIT / ABORT
```

---

# Step 1: Create New Spring Boot Project

Project name:

```text
ecommerce-transaction-ui
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
src/main/java/com/example/ecommerce/

├── EcommerceTransactionUiApplication.java
├── controller/
│   └── OrderController.java
├── model/
│   ├── OrderEvent.java
│   └── InventoryEvent.java
├── service/
│   └── TransactionalOrderService.java
├── consumer/
│   ├── OrderConsumer.java
│   └── InventoryConsumer.java
└── config/
    └── KafkaTopicConfig.java

src/main/resources/
├── templates/
│   └── order.html
└── application.properties
```

---

# Step 3: application.properties

```properties
spring.application.name=ecommerce-transaction-ui

server.port=8084

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.producer.transaction-id-prefix=ecommerce-tx-

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.properties.isolation.level=read_committed

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

Important line:

```properties
spring.kafka.consumer.properties.isolation.level=read_committed
```

This means consumers will only read committed transaction messages.

---

# Step 4: Main Class

```java
package com.example.ecommerce;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EcommerceTransactionUiApplication {

    public static void main(String[] args) {
        SpringApplication.run(EcommerceTransactionUiApplication.class, args);
    }

}
```

---

# Step 5: KafkaTopicConfig.java

Package:

```text
com.example.ecommerce.config
```

```java
package com.example.ecommerce.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic ordersTopic() {
        return new NewTopic("ecommerce-orders-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic inventoryTopic() {
        return new NewTopic("ecommerce-inventory-topic", 3, (short) 1);
    }
}
```

---

# Step 6: OrderEvent.java

Package:

```text
com.example.ecommerce.model
```

```java
package com.example.ecommerce.model;

public class OrderEvent {

    private String orderId;
    private String customerName;
    private String productName;
    private int quantity;
    private double price;
    private String eventType;

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String customerName, String productName, int quantity, double price, String eventType) {
        this.orderId = orderId;
        this.customerName = customerName;
        this.productName = productName;
        this.quantity = quantity;
        this.price = price;
        this.eventType = eventType;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public String getProductName() {
        return productName;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getPrice() {
        return price;
    }

    public String getEventType() {
        return eventType;
    }
}
```

---

# Step 7: InventoryEvent.java

Package:

```text
com.example.ecommerce.model
```

```java
package com.example.ecommerce.model;

public class InventoryEvent {

    private String inventoryEventId;
    private String orderId;
    private String productName;
    private int quantity;
    private String eventType;

    public InventoryEvent() {
    }

    public InventoryEvent(String inventoryEventId, String orderId, String productName, int quantity, String eventType) {
        this.inventoryEventId = inventoryEventId;
        this.orderId = orderId;
        this.productName = productName;
        this.quantity = quantity;
        this.eventType = eventType;
    }

    public String getInventoryEventId() {
        return inventoryEventId;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getProductName() {
        return productName;
    }

    public int getQuantity() {
        return quantity;
    }

    public String getEventType() {
        return eventType;
    }
}
```

---

# Step 8: TransactionalOrderService.java

Package:

```text
com.example.ecommerce.service
```

```java
package com.example.ecommerce.service;

import com.example.ecommerce.model.InventoryEvent;
import com.example.ecommerce.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class TransactionalOrderService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TransactionalOrderService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Transactional
    public String placeOrder(
            String customerName,
            String productName,
            int quantity,
            double price,
            boolean simulateFailure) {

        try {
            String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 5);

            OrderEvent orderEvent = new OrderEvent(
                    orderId,
                    customerName,
                    productName,
                    quantity,
                    price,
                    "ORDER_PLACED"
            );

            InventoryEvent inventoryEvent = new InventoryEvent(
                    "INV-" + UUID.randomUUID().toString().substring(0, 5),
                    orderId,
                    productName,
                    quantity,
                    "INVENTORY_RESERVED"
            );

            String orderJson = objectMapper.writeValueAsString(orderEvent);
            String inventoryJson = objectMapper.writeValueAsString(inventoryEvent);

            kafkaTemplate.send(
                    "ecommerce-orders-topic",
                    orderId,
                    orderJson
            );

            System.out.println("Order event sent inside transaction:");
            System.out.println(orderJson);

            if (simulateFailure) {
                throw new RuntimeException("Simulated failure after order event. Transaction should rollback.");
            }

            kafkaTemplate.send(
                    "ecommerce-inventory-topic",
                    orderId,
                    inventoryJson
            );

            System.out.println("Inventory event sent inside transaction:");
            System.out.println(inventoryJson);

            return orderId;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
```

---

# Step 9: OrderController.java

Package:

```text
com.example.ecommerce.controller
```

```java
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
            model.addAttribute("errorMessage", "Transaction aborted. No partial event should be visible.");
            model.addAttribute("errorDetails", e.getMessage());
        }

        return "order";
    }
}
```

---

# Step 10: OrderConsumer.java

Package:

```text
com.example.ecommerce.consumer
```

```java
package com.example.ecommerce.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    @KafkaListener(topics = "ecommerce-orders-topic", groupId = "order-consumer-group")
    public void consume(ConsumerRecord<String, String> record) {

        System.out.println("Order Consumer Received COMMITTED Event");
        System.out.println("Topic: " + record.topic());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("--------------------------------------");
    }
}
```

---

# Step 11: InventoryConsumer.java

Package:

```text
com.example.ecommerce.consumer
```

```java
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
```

---

# Step 12: order.html

File:

```text
src/main/resources/templates/order.html
```

```html
<!DOCTYPE html>
<html xmlns:th="http://www.thymeleaf.org">
<head>
    <title>E-Commerce Kafka Transaction Publisher</title>

    <style>
        body {
            font-family: Arial;
            background-color: #f4f6f8;
            margin: 40px;
        }

        h1 {
            color: #333;
        }

        .container {
            display: flex;
            gap: 35px;
        }

        .card {
            width: 470px;
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,.1);
        }

        input, select {
            width: 100%;
            padding: 12px;
            margin-top: 12px;
            box-sizing: border-box;
        }

        button {
            width: 100%;
            margin-top: 20px;
            padding: 12px;
            background: #1565c0;
            color: white;
            border: none;
            cursor: pointer;
            font-size: 16px;
            border-radius: 5px;
        }

        button:hover {
            background: #0d47a1;
        }

        .success {
            margin-top: 20px;
            background: #e8f5e9;
            color: #1b5e20;
            padding: 15px;
            border-radius: 8px;
        }

        .error {
            margin-top: 20px;
            background: #ffebee;
            color: #b71c1c;
            padding: 15px;
            border-radius: 8px;
        }

        .info {
            background: #e3f2fd;
            padding: 18px;
            border-radius: 8px;
            line-height: 1.7;
        }

        code {
            background: #eeeeee;
            padding: 3px 7px;
        }

        pre {
            background: #f7f7f7;
            padding: 15px;
            border-radius: 8px;
            overflow-x: auto;
        }

        .checkbox-row {
            margin-top: 15px;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .checkbox-row input {
            width: auto;
            margin-top: 0;
        }
    </style>
</head>
<body>

<h1>E-Commerce Kafka Transaction Publisher</h1>

<div class="container">

    <div class="card">

        <h2>Place Order</h2>

        <form method="post" action="/place-order">

            <label>Customer Name</label>
            <input type="text" name="customerName" placeholder="Customer Name" required>

            <label>Product Name</label>
            <select name="productName" required>
                <option value="">Select Product</option>
                <option value="Laptop">Laptop</option>
                <option value="Mobile Phone">Mobile Phone</option>
                <option value="Headphones">Headphones</option>
                <option value="Keyboard">Keyboard</option>
                <option value="Monitor">Monitor</option>
            </select>

            <label>Quantity</label>
            <input type="number" name="quantity" placeholder="Quantity" required>

            <label>Price</label>
            <input type="number" step="0.01" name="price" placeholder="Price" required>

            <div class="checkbox-row">
                <input type="checkbox" name="simulateFailure" value="true">
                <label>Simulate failure after order event</label>
            </div>

            <button type="submit">Place Order Transactionally</button>

        </form>

        <div class="success" th:if="${successMessage}">
            <h3>Transaction Committed</h3>
            <p th:text="${successMessage}"></p>
            <p>Order ID: <b th:text="${orderId}"></b></p>
        </div>

        <div class="error" th:if="${errorMessage}">
            <h3>Transaction Aborted</h3>
            <p th:text="${errorMessage}"></p>
            <p th:text="${errorDetails}"></p>
        </div>

    </div>

    <div class="card">

        <h2>Kafka Transaction Concept</h2>

        <div class="info">
            <p><b>Topic 1:</b> <code>ecommerce-orders-topic</code></p>
            <p><b>Topic 2:</b> <code>ecommerce-inventory-topic</code></p>

            <p>
                One button publishes to both topics inside one Kafka transaction.
            </p>

            <hr>

            <h3>Success Case</h3>

            <pre>
BEGIN TRANSACTION

orders-topic
  ORDER_PLACED

inventory-topic
  INVENTORY_RESERVED

COMMIT
            </pre>

            <h3>Failure Case</h3>

            <pre>
BEGIN TRANSACTION

orders-topic
  ORDER_PLACED

Simulated Failure

ABORT

No committed event should be visible
            </pre>

            <h3>Key Learning</h3>

            <ul>
                <li>Kafka can publish atomically to multiple topics.</li>
                <li>Either all events are committed or none are visible.</li>
                <li>Consumers with read_committed only read committed records.</li>
                <li>This prevents partial event publishing.</li>
            </ul>
        </div>

    </div>

</div>

</body>
</html>
```

---

# Step 13: Start Kafka

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

# Step 14: Delete Old Topics If Needed

```bash
kafka-topics.sh --delete --topic ecommerce-orders-topic --bootstrap-server localhost:9092
kafka-topics.sh --delete --topic ecommerce-inventory-topic --bootstrap-server localhost:9092
```

Then run the app. Spring will recreate topics.

Or manually create:

```bash
kafka-topics.sh --create --topic ecommerce-orders-topic --partitions 3 --bootstrap-server localhost:9092

kafka-topics.sh --create --topic ecommerce-inventory-topic --partitions 3 --bootstrap-server localhost:9092
```

---

# Step 15: Run Application

Run:

```text
EcommerceTransactionUiApplication
```

Open:

```text
http://localhost:8084
```

---

# Step 16: Success Test

Enter:

```text
Customer Name: Abdullah
Product: Laptop
Quantity: 1
Price: 60000
```

Do **not** check simulate failure.

Click:

```text
Place Order Transactionally
```

Expected console:

```text
Order event sent inside transaction:
{...ORDER_PLACED...}

Inventory event sent inside transaction:
{...INVENTORY_RESERVED...}

Order Consumer Received COMMITTED Event
...

Inventory Consumer Received COMMITTED Event
...
```

---

# Step 17: Failure Test

Enter:

```text
Customer Name: Ahmed
Product: Mobile Phone
Quantity: 1
Price: 25000
```

Check:

```text
Simulate failure after order event
```

Expected UI:

```text
Transaction Aborted
No partial event should be visible
```

Expected console:

```text
Order event sent inside transaction:
{...ORDER_PLACED...}

Transaction aborted
```

Important: `OrderConsumer` should **not** receive this failed order because it was not committed.

---

# Step 18: Teaching Explanation

Without Kafka transaction:

```text
orders-topic receives ORDER_PLACED
inventory-topic fails
system becomes inconsistent
```

With Kafka transaction:

```text
BEGIN
send orders-topic
send inventory-topic
COMMIT
```

If failure:

```text
BEGIN
send orders-topic
failure
ABORT
```

Consumers using:

```properties
isolation.level=read_committed
```

only see committed events.
