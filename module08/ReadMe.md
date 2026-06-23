
# UI-8: Delivery Status Tracking using Multiple Events

## Business Scenario

A retail/e-commerce company wants to track an order delivery lifecycle.

The order moves through multiple delivery events:

```text
ORDER_PLACED
ORDER_PACKED
ORDER_SHIPPED
OUT_FOR_DELIVERY
DELIVERED
```

Each status change is published as a Kafka event.

The UI shows:

```text
Current delivery status
Full event history
Kafka topic
Partition
Offset
Event type
```

---

# Step 1: Create Spring Boot Project

Project name:

```text
delivery-status-tracking-ui
```

Dependencies:

```text
Spring Web
Spring for Apache Kafka
Thymeleaf
Lombok
```

Use Java 17 or Java 21.

---

# Step 2: Project Structure

```text
src/main/java/com/example/delivery/

├── DeliveryStatusTrackingUiApplication.java

├── config/
│   └── KafkaTopicConfig.java

├── controller/
│   └── DeliveryController.java

├── model/
│   ├── DeliveryEvent.java
│   ├── DeliveryStatusView.java
│   └── DeliveryEventLog.java

├── producer/
│   └── DeliveryEventProducer.java

└── consumer/
    └── DeliveryStatusConsumer.java

src/main/resources/
├── templates/
│   └── delivery.html
└── application.properties
```

---

# Step 3: pom.xml

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">

    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.4.5</version>
        <relativePath/>
    </parent>

    <groupId>com.example</groupId>
    <artifactId>delivery-status-tracking-ui</artifactId>
    <version>0.0.1-SNAPSHOT</version>

    <name>delivery-status-tracking-ui</name>

    <properties>
        <java.version>17</java.version>
    </properties>

    <dependencies>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-thymeleaf</artifactId>
        </dependency>

        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>

    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>

</project>
```

Important: use this dependency:

```xml
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
</dependency>
```

Do **not** use:

```text
spring-boot-starter-kafka
```

---

# Step 4: application.properties

File:

```text
src/main/resources/application.properties
```

```properties
spring.application.name=delivery-status-tracking-ui

server.port=8087

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer

spring.kafka.consumer.auto-offset-reset=earliest
```

---

# Step 5: Main Class

File:

```text
src/main/java/com/example/delivery/DeliveryStatusTrackingUiApplication.java
```

```java
package com.example.delivery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DeliveryStatusTrackingUiApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeliveryStatusTrackingUiApplication.class, args);
    }
}
```

---

# Step 6: KafkaTopicConfig.java

File:

```text
src/main/java/com/example/delivery/config/KafkaTopicConfig.java
```

```java
package com.example.delivery.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic deliveryStatusTopic() {
        return new NewTopic("delivery-status-topic", 3, (short) 1);
    }
}
```

The topic has 3 partitions.

Since we send Kafka events with `orderId` as the message key, events for the same order usually go to the same partition. This helps maintain order-wise event sequence.

---

# Step 7: DeliveryEvent.java

File:

```text
src/main/java/com/example/delivery/model/DeliveryEvent.java
```

```java
package com.example.delivery.model;

public class DeliveryEvent {

    private String eventId;
    private String orderId;
    private String customerName;
    private String productName;
    private String deliveryStatus;
    private String eventType;
    private String eventTime;

    public DeliveryEvent() {
    }

    public DeliveryEvent(
            String eventId,
            String orderId,
            String customerName,
            String productName,
            String deliveryStatus,
            String eventType,
            String eventTime) {

        this.eventId = eventId;
        this.orderId = orderId;
        this.customerName = customerName;
        this.productName = productName;
        this.deliveryStatus = deliveryStatus;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getDeliveryStatus() {
        return deliveryStatus;
    }

    public void setDeliveryStatus(String deliveryStatus) {
        this.deliveryStatus = deliveryStatus;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }
}
```

---

# Step 8: DeliveryStatusView.java

This class stores the latest status of each order.

File:

```text
src/main/java/com/example/delivery/model/DeliveryStatusView.java
```

```java
package com.example.delivery.model;

public class DeliveryStatusView {

    private String orderId;
    private String customerName;
    private String productName;
    private String currentStatus;
    private String lastUpdatedTime;

    public DeliveryStatusView() {
    }

    public DeliveryStatusView(
            String orderId,
            String customerName,
            String productName,
            String currentStatus,
            String lastUpdatedTime) {

        this.orderId = orderId;
        this.customerName = customerName;
        this.productName = productName;
        this.currentStatus = currentStatus;
        this.lastUpdatedTime = lastUpdatedTime;
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

    public String getCurrentStatus() {
        return currentStatus;
    }

    public String getLastUpdatedTime() {
        return lastUpdatedTime;
    }
}
```

---

# Step 9: DeliveryEventLog.java

This class stores event history for the UI.

File:

```text
src/main/java/com/example/delivery/model/DeliveryEventLog.java
```

```java
package com.example.delivery.model;

public class DeliveryEventLog {

    private String orderId;
    private String eventType;
    private String deliveryStatus;
    private String customerName;
    private String productName;
    private String topic;
    private int partition;
    private long offset;
    private String eventTime;

    public DeliveryEventLog() {
    }

    public DeliveryEventLog(
            String orderId,
            String eventType,
            String deliveryStatus,
            String customerName,
            String productName,
            String topic,
            int partition,
            long offset,
            String eventTime) {

        this.orderId = orderId;
        this.eventType = eventType;
        this.deliveryStatus = deliveryStatus;
        this.customerName = customerName;
        this.productName = productName;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.eventTime = eventTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getDeliveryStatus() {
        return deliveryStatus;
    }

    public String getCustomerName() {
        return customerName;
    }

    public String getProductName() {
        return productName;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public String getEventTime() {
        return eventTime;
    }
}
```

---

# Step 10: DeliveryEventProducer.java

File:

```text
src/main/java/com/example/delivery/producer/DeliveryEventProducer.java
```

```java
package com.example.delivery.producer;

import com.example.delivery.model.DeliveryEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class DeliveryEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DeliveryEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String createOrder(
            String customerName,
            String productName) {

        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();

        publishDeliveryEvent(
                orderId,
                customerName,
                productName,
                "ORDER_PLACED",
                "ORDER_PLACED"
        );

        return orderId;
    }

    public void updateDeliveryStatus(
            String orderId,
            String customerName,
            String productName,
            String deliveryStatus) {

        publishDeliveryEvent(
                orderId,
                customerName,
                productName,
                deliveryStatus,
                deliveryStatus
        );
    }

    private void publishDeliveryEvent(
            String orderId,
            String customerName,
            String productName,
            String deliveryStatus,
            String eventType) {

        try {
            String eventId = "EVT-" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();

            DeliveryEvent deliveryEvent = new DeliveryEvent(
                    eventId,
                    orderId,
                    customerName,
                    productName,
                    deliveryStatus,
                    eventType,
                    LocalDateTime.now().toString()
            );

            String eventJson = objectMapper.writeValueAsString(deliveryEvent);

            kafkaTemplate.send(
                    "delivery-status-topic",
                    orderId,
                    eventJson
            );

            System.out.println("Delivery Event Published:");
            System.out.println("Key: " + orderId);
            System.out.println("Value: " + eventJson);
            System.out.println("--------------------------------");

        } catch (Exception e) {
            throw new RuntimeException("Error publishing delivery event", e);
        }
    }
}
```

---

# Step 11: DeliveryStatusConsumer.java

This consumer reads delivery events and updates the in-memory dashboard.

File:

```text
src/main/java/com/example/delivery/consumer/DeliveryStatusConsumer.java
```

```java
package com.example.delivery.consumer;

import com.example.delivery.model.DeliveryEvent;
import com.example.delivery.model.DeliveryEventLog;
import com.example.delivery.model.DeliveryStatusView;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DeliveryStatusConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Map<String, DeliveryStatusView> currentStatusMap =
            new ConcurrentHashMap<>();

    private final List<DeliveryEventLog> eventLogs =
            Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(
            topics = "delivery-status-topic",
            groupId = "delivery-dashboard-group"
    )
    public void consume(ConsumerRecord<String, String> record) throws Exception {

        DeliveryEvent deliveryEvent =
                objectMapper.readValue(record.value(), DeliveryEvent.class);

        DeliveryStatusView statusView =
                new DeliveryStatusView(
                        deliveryEvent.getOrderId(),
                        deliveryEvent.getCustomerName(),
                        deliveryEvent.getProductName(),
                        deliveryEvent.getDeliveryStatus(),
                        deliveryEvent.getEventTime()
                );

        currentStatusMap.put(
                deliveryEvent.getOrderId(),
                statusView
        );

        DeliveryEventLog eventLog =
                new DeliveryEventLog(
                        deliveryEvent.getOrderId(),
                        deliveryEvent.getEventType(),
                        deliveryEvent.getDeliveryStatus(),
                        deliveryEvent.getCustomerName(),
                        deliveryEvent.getProductName(),
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        deliveryEvent.getEventTime()
                );

        eventLogs.add(0, eventLog);

        if (eventLogs.size() > 50) {
            eventLogs.remove(eventLogs.size() - 1);
        }

        System.out.println("Delivery Dashboard Updated:");
        System.out.println("Order ID: " + deliveryEvent.getOrderId());
        System.out.println("Status: " + deliveryEvent.getDeliveryStatus());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("--------------------------------");
    }

    public Map<String, DeliveryStatusView> getCurrentStatusMap() {
        return currentStatusMap;
    }

    public List<DeliveryEventLog> getEventLogs() {
        return eventLogs;
    }

    public void clearLogs() {
        currentStatusMap.clear();
        eventLogs.clear();
    }
}
```

---

# Step 12: DeliveryController.java

File:

```text
src/main/java/com/example/delivery/controller/DeliveryController.java
```

```java
package com.example.delivery.controller;

import com.example.delivery.consumer.DeliveryStatusConsumer;
import com.example.delivery.producer.DeliveryEventProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
public class DeliveryController {

    private final DeliveryEventProducer deliveryEventProducer;
    private final DeliveryStatusConsumer deliveryStatusConsumer;

    public DeliveryController(
            DeliveryEventProducer deliveryEventProducer,
            DeliveryStatusConsumer deliveryStatusConsumer) {

        this.deliveryEventProducer = deliveryEventProducer;
        this.deliveryStatusConsumer = deliveryStatusConsumer;
    }

    @GetMapping("/")
    public String showPage(Model model) {

        model.addAttribute(
                "orders",
                deliveryStatusConsumer.getCurrentStatusMap()
        );

        model.addAttribute(
                "logs",
                deliveryStatusConsumer.getEventLogs()
        );

        return "delivery";
    }

    @PostMapping("/orders")
    public String createOrder(
            @RequestParam String customerName,
            @RequestParam String productName,
            Model model) {

        String orderId =
                deliveryEventProducer.createOrder(
                        customerName,
                        productName
                );

        waitForConsumer();

        model.addAttribute("message", "Order created and ORDER_PLACED event published.");
        model.addAttribute("orderId", orderId);

        model.addAttribute(
                "orders",
                deliveryStatusConsumer.getCurrentStatusMap()
        );

        model.addAttribute(
                "logs",
                deliveryStatusConsumer.getEventLogs()
        );

        return "delivery";
    }

    @PostMapping("/delivery-status")
    public String updateDeliveryStatus(
            @RequestParam String orderId,
            @RequestParam String customerName,
            @RequestParam String productName,
            @RequestParam String deliveryStatus,
            Model model) {

        deliveryEventProducer.updateDeliveryStatus(
                orderId,
                customerName,
                productName,
                deliveryStatus
        );

        waitForConsumer();

        model.addAttribute("message", "Delivery status event published.");
        model.addAttribute("orderId", orderId);

        model.addAttribute(
                "orders",
                deliveryStatusConsumer.getCurrentStatusMap()
        );

        model.addAttribute(
                "logs",
                deliveryStatusConsumer.getEventLogs()
        );

        return "delivery";
    }

    @PostMapping("/clear")
    public String clearDashboard() {
        deliveryStatusConsumer.clearLogs();
        return "redirect:/";
    }

    private void waitForConsumer() {
        try {
            Thread.sleep(800);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
```

---

# Step 13: delivery.html

File:

```text
src/main/resources/templates/delivery.html
```

```html
<!DOCTYPE html>
<html xmlns:th="http://www.thymeleaf.org">
<head>
    <title>Delivery Status Tracking using Kafka Events</title>

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
            gap: 30px;
            align-items: flex-start;
        }

        .card {
            width: 430px;
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,.1);
            margin-bottom: 25px;
        }

        .wide-card {
            flex: 1;
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,.1);
            margin-bottom: 25px;
        }

        input, select {
            width: 100%;
            padding: 12px;
            margin-top: 10px;
            margin-bottom: 15px;
            box-sizing: border-box;
        }

        label {
            font-weight: bold;
        }

        button {
            width: 100%;
            margin-top: 10px;
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

        .danger {
            background: #c62828;
        }

        .danger:hover {
            background: #8e0000;
        }

        .success {
            margin-bottom: 20px;
            background: #e0f2f1;
            color: #004d40;
            padding: 15px;
            border-radius: 8px;
        }

        .info {
            background: #e3f2fd;
            padding: 18px;
            border-radius: 8px;
            line-height: 1.7;
            margin-bottom: 25px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
            font-size: 14px;
        }

        th, td {
            padding: 11px;
            border-bottom: 1px solid #ddd;
            text-align: left;
        }

        th {
            background-color: #eeeeee;
        }

        code {
            background: #eeeeee;
            padding: 3px 7px;
        }

        pre {
            background: #f7f7f7;
            padding: 15px;
            border-radius: 8px;
        }

        .status {
            font-weight: bold;
            color: #1565c0;
        }
    </style>
</head>

<body>

<h1>UI-7: Delivery Status Tracking using Multiple Kafka Events</h1>

<div class="info">
    <h3>Kafka Concept</h3>

    <p>
        This UI publishes multiple delivery events for the same order into
        <code>delivery-status-topic</code>.
    </p>

    <pre>
ORDER_PLACED
    ↓
ORDER_PACKED
    ↓
ORDER_SHIPPED
    ↓
OUT_FOR_DELIVERY
    ↓
DELIVERED
    </pre>

    <p>
        Every status change is a separate Kafka event.
        The dashboard consumer reads the events and updates the latest status.
    </p>
</div>

<div th:if="${message}" class="success">
    <h3>Kafka Event Published</h3>
    <p th:text="${message}"></p>
    <p>Order ID: <b th:text="${orderId}"></b></p>
</div>

<div class="container">

    <div>

        <div class="card">
            <h2>Create Order</h2>

            <form method="post" action="/orders">

                <label>Customer Name</label>
                <input type="text" name="customerName" value="Abdullah" required>

                <label>Product Name</label>
                <input type="text" name="productName" value="Laptop" required>

                <button type="submit">Create Order</button>

            </form>
        </div>

        <div class="card">
            <h2>Update Delivery Status</h2>

            <form method="post" action="/delivery-status">

                <label>Order ID</label>
                <input type="text" name="orderId" placeholder="Example: ORD-ABCDE" required>

                <label>Customer Name</label>
                <input type="text" name="customerName" value="Abdullah" required>

                <label>Product Name</label>
                <input type="text" name="productName" value="Laptop" required>

                <label>Delivery Status</label>
                <select name="deliveryStatus" required>
                    <option value="">Select Status</option>
                    <option value="ORDER_PACKED">ORDER_PACKED</option>
                    <option value="ORDER_SHIPPED">ORDER_SHIPPED</option>
                    <option value="OUT_FOR_DELIVERY">OUT_FOR_DELIVERY</option>
                    <option value="DELIVERED">DELIVERED</option>
                </select>

                <button type="submit">Publish Status Event</button>

            </form>
        </div>

        <div class="card">
            <h2>Clear Dashboard</h2>

            <form method="post" action="/clear">
                <button class="danger" type="submit">Clear All Data</button>
            </form>
        </div>

    </div>

    <div class="wide-card">

        <h2>Current Delivery Status</h2>

        <table>
            <thead>
            <tr>
                <th>Order ID</th>
                <th>Customer</th>
                <th>Product</th>
                <th>Current Status</th>
                <th>Last Updated</th>
            </tr>
            </thead>

            <tbody>
            <tr th:each="entry : ${orders}">
                <td th:text="${entry.value.orderId}"></td>
                <td th:text="${entry.value.customerName}"></td>
                <td th:text="${entry.value.productName}"></td>
                <td class="status" th:text="${entry.value.currentStatus}"></td>
                <td th:text="${entry.value.lastUpdatedTime}"></td>
            </tr>
            </tbody>
        </table>

        <h2>Kafka Event History</h2>

        <table>
            <thead>
            <tr>
                <th>Order ID</th>
                <th>Event Type</th>
                <th>Status</th>
                <th>Topic</th>
                <th>Partition</th>
                <th>Offset</th>
                <th>Event Time</th>
            </tr>
            </thead>

            <tbody>
            <tr th:each="log : ${logs}">
                <td th:text="${log.orderId}"></td>
                <td th:text="${log.eventType}"></td>
                <td class="status" th:text="${log.deliveryStatus}"></td>
                <td th:text="${log.topic}"></td>
                <td th:text="${log.partition}"></td>
                <td th:text="${log.offset}"></td>
                <td th:text="${log.eventTime}"></td>
            </tr>
            </tbody>
        </table>

    </div>

</div>

</body>
</html>
```

---

# Step 14: Start Kafka

If Kafka and Zookeeper containers are already created, run:

```bash
docker start zookeeper
sleep 15
docker start kafka
sleep 15
docker ps
```

Verify topic list:

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:9092
```

After the Spring Boot app starts, you should see:

```text
delivery-status-topic
```

---

# Step 15: Run App

Run the main class:

```text
DeliveryStatusTrackingUiApplication
```

Or from terminal:

```bash
mvn clean spring-boot:run
```

Open:

```text
http://localhost:8087
```

---

# Step 16: Test

## Test 1: Create Order

Enter:

```text
Customer Name: Abdullah
Product Name: Laptop
```

Click:

```text
Create Order
```

Expected success message:

```text
Order created and ORDER_PLACED event published.
Order ID: ORD-XXXXX
```

Expected current status table:

```text
ORD-XXXXX    Abdullah    Laptop    ORDER_PLACED
```

Expected event history:

```text
ORD-XXXXX    ORDER_PLACED    ORDER_PLACED    delivery-status-topic    partition    offset
```

---

## Test 2: Publish ORDER_PACKED Event

Copy the generated order ID.

Enter:

```text
Order ID: ORD-XXXXX
Customer Name: Abdullah
Product Name: Laptop
Delivery Status: ORDER_PACKED
```

Click:

```text
Publish Status Event
```

Expected current status:

```text
ORD-XXXXX    Abdullah    Laptop    ORDER_PACKED
```

Expected event history:

```text
ORDER_PACKED
ORDER_PLACED
```

---

## Test 3: Publish ORDER_SHIPPED Event

Select:

```text
ORDER_SHIPPED
```

Expected current status:

```text
ORD-XXXXX    Abdullah    Laptop    ORDER_SHIPPED
```

Expected event history:

```text
ORDER_SHIPPED
ORDER_PACKED
ORDER_PLACED
```

---

## Test 4: Publish OUT_FOR_DELIVERY Event

Select:

```text
OUT_FOR_DELIVERY
```

Expected current status:

```text
ORD-XXXXX    Abdullah    Laptop    OUT_FOR_DELIVERY
```

---

## Test 5: Publish DELIVERED Event

Select:

```text
DELIVERED
```

Expected current status:

```text
ORD-XXXXX    Abdullah    Laptop    DELIVERED
```

Expected event history:

```text
DELIVERED
OUT_FOR_DELIVERY
ORDER_SHIPPED
ORDER_PACKED
ORDER_PLACED
```

---

# Expected Console Output

```text
Delivery Event Published:
Key: ORD-7A91B
Value: {"eventId":"EVT-2F13A","orderId":"ORD-7A91B","customerName":"Abdullah","productName":"Laptop","deliveryStatus":"ORDER_PLACED","eventType":"ORDER_PLACED","eventTime":"2026-06-23T10:30:15.123"}
--------------------------------
```

Consumer output:

```text
Delivery Dashboard Updated:
Order ID: ORD-7A91B
Status: ORDER_PLACED
Partition: 1
Offset: 0
--------------------------------
```

After more status updates:

```text
Delivery Dashboard Updated:
Order ID: ORD-7A91B
Status: ORDER_PACKED
Partition: 1
Offset: 1
--------------------------------

Delivery Dashboard Updated:
Order ID: ORD-7A91B
Status: ORDER_SHIPPED
Partition: 1
Offset: 2
--------------------------------
```

---
