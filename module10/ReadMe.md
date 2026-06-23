
# UI-12: Consumer Lag Dashboard

## Business Scenario

An order system is producing events quickly.

A reporting consumer is intentionally slow.

The UI will show:

```text
Produced message count
Consumed message count
Estimated consumer lag
Latest consumed events
Kafka partition
Kafka offset
```

---

## IMPORTANT CLARIFICATION

```text
Kafka feature:
Topic, partition, offset, consumer group, lag.

Application feature:
UI dashboard, produced count, consumed count, in-memory event list.
```

Consumer lag is calculated using Kafka offsets:

```text
Lag = Latest Kafka offset - Consumer group committed offset
```

But in this simple UI, we will also show an **approximate app-level lag**:

```text
Produced count - Consumed count
```

This is good for classroom visualization.

---

# Step 1: Create Spring Boot Project

Project name:

```text
consumer-lag-dashboard-ui
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
src/main/java/com/example/lagdashboard/

├── ConsumerLagDashboardUiApplication.java

├── config/
│   └── KafkaTopicConfig.java

├── controller/
│   └── LagDashboardController.java

├── model/
│   ├── OrderEvent.java
│   ├── ConsumedOrderLog.java
│   └── LagSummary.java

├── producer/
│   └── OrderEventProducer.java

└── consumer/
    └── SlowOrderConsumer.java

src/main/resources/
├── templates/
│   └── lag-dashboard.html
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
    <artifactId>consumer-lag-dashboard-ui</artifactId>
    <version>0.0.1-SNAPSHOT</version>

    <name>consumer-lag-dashboard-ui</name>

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

Use:

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
spring.application.name=consumer-lag-dashboard-ui

server.port=8092

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer

spring.kafka.consumer.auto-offset-reset=earliest

spring.kafka.listener.ack-mode=record
```

---

# Step 5: Main Class

File:

```text
src/main/java/com/example/lagdashboard/ConsumerLagDashboardUiApplication.java
```

```java
package com.example.lagdashboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ConsumerLagDashboardUiApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerLagDashboardUiApplication.class, args);
    }
}
```

---

# Step 6: KafkaTopicConfig.java

File:

```text
src/main/java/com/example/lagdashboard/config/KafkaTopicConfig.java
```

```java
package com.example.lagdashboard.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic ordersLagTopic() {
        return new NewTopic("orders-lag-topic", 3, (short) 1);
    }
}
```

---

# Step 7: OrderEvent.java

File:

```text
src/main/java/com/example/lagdashboard/model/OrderEvent.java
```

```java
package com.example.lagdashboard.model;

public class OrderEvent {

    private String orderId;
    private String customerName;
    private String productName;
    private double amount;
    private String eventType;
    private String eventTime;

    public OrderEvent() {
    }

    public OrderEvent(
            String orderId,
            String customerName,
            String productName,
            double amount,
            String eventType,
            String eventTime) {

        this.orderId = orderId;
        this.customerName = customerName;
        this.productName = productName;
        this.amount = amount;
        this.eventType = eventType;
        this.eventTime = eventTime;
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

    public double getAmount() {
        return amount;
    }

    public String getEventType() {
        return eventType;
    }

    public String getEventTime() {
        return eventTime;
    }
}
```

---

# Step 8: ConsumedOrderLog.java

This stores consumed events for the UI.

## IMPORTANT CLARIFICATION

```text
This is not Kafka's internal log.

This is an application-level list used only for browser display.

Kafka's real event log is inside topic partitions.
```

File:

```text
src/main/java/com/example/lagdashboard/model/ConsumedOrderLog.java
```

```java
package com.example.lagdashboard.model;

public class ConsumedOrderLog {

    private String orderId;
    private String customerName;
    private String productName;
    private double amount;
    private String topic;
    private int partition;
    private long offset;
    private String consumedTime;

    public ConsumedOrderLog() {
    }

    public ConsumedOrderLog(
            String orderId,
            String customerName,
            String productName,
            double amount,
            String topic,
            int partition,
            long offset,
            String consumedTime) {

        this.orderId = orderId;
        this.customerName = customerName;
        this.productName = productName;
        this.amount = amount;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.consumedTime = consumedTime;
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

    public double getAmount() {
        return amount;
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

    public String getConsumedTime() {
        return consumedTime;
    }
}
```

---

# Step 9: LagSummary.java

File:

```text
src/main/java/com/example/lagdashboard/model/LagSummary.java
```

```java
package com.example.lagdashboard.model;

public class LagSummary {

    private int producedCount;
    private int consumedCount;
    private int estimatedLag;

    public LagSummary() {
    }

    public LagSummary(
            int producedCount,
            int consumedCount,
            int estimatedLag) {

        this.producedCount = producedCount;
        this.consumedCount = consumedCount;
        this.estimatedLag = estimatedLag;
    }

    public int getProducedCount() {
        return producedCount;
    }

    public int getConsumedCount() {
        return consumedCount;
    }

    public int getEstimatedLag() {
        return estimatedLag;
    }
}
```

---

# Step 10: OrderEventProducer.java

File:

```text
src/main/java/com/example/lagdashboard/producer/OrderEventProducer.java
```

```java
package com.example.lagdashboard.producer;

import com.example.lagdashboard.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class OrderEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AtomicInteger producedCount = new AtomicInteger(0);

    public OrderEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishSingleOrder(
            String customerName,
            String productName,
            double amount) {

        publishOrder(customerName, productName, amount);
    }

    public void publishBulkOrders(int count) {

        for (int i = 1; i <= count; i++) {
            publishOrder(
                    "Customer-" + i,
                    "Product-" + i,
                    1000 + i
            );
        }
    }

    private void publishOrder(
            String customerName,
            String productName,
            double amount) {

        try {
            String orderId =
                    "ORD-" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();

            OrderEvent orderEvent =
                    new OrderEvent(
                            orderId,
                            customerName,
                            productName,
                            amount,
                            "ORDER_CREATED",
                            LocalDateTime.now().toString()
                    );

            String eventJson =
                    objectMapper.writeValueAsString(orderEvent);

            kafkaTemplate.send(
                    "orders-lag-topic",
                    orderId,
                    eventJson
            );

            producedCount.incrementAndGet();

            System.out.println("Order Event Published:");
            System.out.println("Key: " + orderId);
            System.out.println("Value: " + eventJson);
            System.out.println("--------------------------------");

        } catch (Exception e) {
            throw new RuntimeException("Error publishing order event", e);
        }
    }

    public int getProducedCount() {
        return producedCount.get();
    }

    public void resetProducedCount() {
        producedCount.set(0);
    }
}
```

---

# Step 11: SlowOrderConsumer.java

This consumer intentionally sleeps before processing each message.

File:

```text
src/main/java/com/example/lagdashboard/consumer/SlowOrderConsumer.java
```

```java
package com.example.lagdashboard.consumer;

import com.example.lagdashboard.model.ConsumedOrderLog;
import com.example.lagdashboard.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SlowOrderConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final List<ConsumedOrderLog> consumedLogs =
            Collections.synchronizedList(new ArrayList<>());

    private final AtomicInteger consumedCount = new AtomicInteger(0);

    @KafkaListener(
            topics = "orders-lag-topic",
            groupId = "slow-reporting-group"
    )
    public void consume(ConsumerRecord<String, String> record)
            throws Exception {

        Thread.sleep(2000);

        OrderEvent orderEvent =
                objectMapper.readValue(record.value(), OrderEvent.class);

        ConsumedOrderLog consumedOrderLog =
                new ConsumedOrderLog(
                        orderEvent.getOrderId(),
                        orderEvent.getCustomerName(),
                        orderEvent.getProductName(),
                        orderEvent.getAmount(),
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        LocalDateTime.now().toString()
                );

        consumedLogs.add(0, consumedOrderLog);

        if (consumedLogs.size() > 50) {
            consumedLogs.remove(consumedLogs.size() - 1);
        }

        consumedCount.incrementAndGet();

        System.out.println("Slow Consumer Processed Order:");
        System.out.println("Order ID: " + orderEvent.getOrderId());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("Consumed Count: " + consumedCount.get());
        System.out.println("--------------------------------");
    }

    public List<ConsumedOrderLog> getConsumedLogs() {
        return consumedLogs;
    }

    public int getConsumedCount() {
        return consumedCount.get();
    }

    public void clearConsumerView() {
        consumedLogs.clear();
        consumedCount.set(0);
    }
}
```

## Important point

```text
Thread.sleep(2000) creates artificial slowness.

This is not a Kafka feature.

This is only for classroom demonstration so we can see lag building up.
```

---

# Step 12: LagDashboardController.java

File:

```text
src/main/java/com/example/lagdashboard/controller/LagDashboardController.java
```

```java
package com.example.lagdashboard.controller;

import com.example.lagdashboard.consumer.SlowOrderConsumer;
import com.example.lagdashboard.model.LagSummary;
import com.example.lagdashboard.producer.OrderEventProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
public class LagDashboardController {

    private final OrderEventProducer orderEventProducer;
    private final SlowOrderConsumer slowOrderConsumer;

    public LagDashboardController(
            OrderEventProducer orderEventProducer,
            SlowOrderConsumer slowOrderConsumer) {

        this.orderEventProducer = orderEventProducer;
        this.slowOrderConsumer = slowOrderConsumer;
    }

    @GetMapping("/")
    public String showDashboard(Model model) {

        addDashboardData(model);

        return "lag-dashboard";
    }

    @PostMapping("/orders/single")
    public String publishSingleOrder(
            @RequestParam String customerName,
            @RequestParam String productName,
            @RequestParam double amount,
            Model model) {

        orderEventProducer.publishSingleOrder(
                customerName,
                productName,
                amount
        );

        addDashboardData(model);

        model.addAttribute(
                "message",
                "Single order event published to Kafka."
        );

        return "lag-dashboard";
    }

    @PostMapping("/orders/bulk")
    public String publishBulkOrders(
            @RequestParam int count,
            Model model) {

        orderEventProducer.publishBulkOrders(count);

        addDashboardData(model);

        model.addAttribute(
                "message",
                count + " order events published quickly to Kafka."
        );

        return "lag-dashboard";
    }

    @PostMapping("/clear")
    public String clearDashboard() {

        orderEventProducer.resetProducedCount();
        slowOrderConsumer.clearConsumerView();

        return "redirect:/";
    }

    private void addDashboardData(Model model) {

        int producedCount =
                orderEventProducer.getProducedCount();

        int consumedCount =
                slowOrderConsumer.getConsumedCount();

        int estimatedLag =
                producedCount - consumedCount;

        if (estimatedLag < 0) {
            estimatedLag = 0;
        }

        LagSummary lagSummary =
                new LagSummary(
                        producedCount,
                        consumedCount,
                        estimatedLag
                );

        model.addAttribute(
                "lagSummary",
                lagSummary
        );

        model.addAttribute(
                "consumedLogs",
                slowOrderConsumer.getConsumedLogs()
        );
    }
}
```

---

# Step 13: lag-dashboard.html

File:

```text
src/main/resources/templates/lag-dashboard.html
```

```html
<!DOCTYPE html>
<html xmlns:th="http://www.thymeleaf.org">
<head>
    <title>Consumer Lag Dashboard using Kafka</title>

    <meta http-equiv="refresh" content="3">

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

        input {
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

        .clarification {
            background: #fff3cd;
            padding: 18px;
            border-radius: 8px;
            line-height: 1.7;
            margin-bottom: 25px;
        }

        .summary-boxes {
            display: flex;
            gap: 20px;
            margin-bottom: 30px;
        }

        .summary-box {
            flex: 1;
            background: #f7f7f7;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        }

        .summary-number {
            font-size: 34px;
            font-weight: bold;
            color: #1565c0;
        }

        .lag-number {
            font-size: 34px;
            font-weight: bold;
            color: #c62828;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
            font-size: 14px;
            margin-bottom: 35px;
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
    </style>
</head>

<body>

<h1>UI-12: Consumer Lag Dashboard using Kafka</h1>

<div class="info">
    <h3>Kafka Flow</h3>

    <p>
        This UI publishes order events quickly to <code>orders-lag-topic</code>.
        The consumer intentionally processes each message slowly.
    </p>

    <pre>
Order Producer
    ↓
orders-lag-topic
    ↓
SlowOrderConsumer
    ↓
Processing delay
    ↓
Lag becomes visible
    </pre>
</div>

<div class="clarification">
    <h3>Important Clarification</h3>

    <p>
        Consumer lag is a real Kafka concept.
        It is based on offsets.
    </p>

    <p>
        Kafka lag means the difference between the latest offset in the topic
        and the committed offset of the consumer group.
    </p>

    <p>
        The numbers shown in this UI are a simple application-level approximation:
        produced count minus consumed count.
    </p>

    <p>
        For real Kafka lag, use the command:
        <code>kafka-consumer-groups --describe --group slow-reporting-group</code>
    </p>
</div>

<div th:if="${message}" class="success">
    <h3>Kafka Event Published</h3>
    <p th:text="${message}"></p>
</div>

<div class="container">

    <div>

        <div class="card">
            <h2>Publish Single Order</h2>

            <form method="post" action="/orders/single">

                <label>Customer Name</label>
                <input type="text" name="customerName" value="Abdullah" required>

                <label>Product Name</label>
                <input type="text" name="productName" value="Laptop" required>

                <label>Amount</label>
                <input type="number" step="0.01" name="amount" value="65000" required>

                <button type="submit">Publish Single Event</button>

            </form>
        </div>

        <div class="card">
            <h2>Publish Bulk Orders</h2>

            <form method="post" action="/orders/bulk">

                <label>Number of Orders</label>
                <input type="number" name="count" value="10" required>

                <button type="submit">Publish Bulk Events</button>

            </form>
        </div>

        <div class="card">
            <h2>Clear Application View</h2>

            <form method="post" action="/clear">
                <button class="danger" type="submit">Clear Dashboard</button>
            </form>
        </div>

    </div>

    <div class="wide-card">

        <h2>Lag Summary - Application View</h2>

        <div class="summary-boxes">

            <div class="summary-box">
                <h3>Produced</h3>
                <div class="summary-number"
                     th:text="${lagSummary.producedCount}">
                </div>
            </div>

            <div class="summary-box">
                <h3>Consumed</h3>
                <div class="summary-number"
                     th:text="${lagSummary.consumedCount}">
                </div>
            </div>

            <div class="summary-box">
                <h3>Estimated Lag</h3>
                <div class="lag-number"
                     th:text="${lagSummary.estimatedLag}">
                </div>
            </div>

        </div>

        <h2>Consumed Order Events - Application View</h2>

        <p>
            This table is maintained by the Spring Boot application in memory.
            Kafka stores the actual event log inside topic partitions.
        </p>

        <table>
            <thead>
            <tr>
                <th>Order ID</th>
                <th>Customer</th>
                <th>Product</th>
                <th>Amount</th>
                <th>Topic</th>
                <th>Partition</th>
                <th>Offset</th>
                <th>Consumed Time</th>
            </tr>
            </thead>

            <tbody>
            <tr th:each="log : ${consumedLogs}">
                <td th:text="${log.orderId}"></td>
                <td th:text="${log.customerName}"></td>
                <td th:text="${log.productName}"></td>
                <td th:text="${log.amount}"></td>
                <td th:text="${log.topic}"></td>
                <td th:text="${log.partition}"></td>
                <td th:text="${log.offset}"></td>
                <td th:text="${log.consumedTime}"></td>
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

```bash
docker start zookeeper
sleep 15
docker start kafka
sleep 15
docker ps
```

Verify:

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --list
```

After the app starts, you should see:

```text
orders-lag-topic
```

---

# Step 15: Run App

Run main class:

```text
ConsumerLagDashboardUiApplication
```

Or:

```bash
mvn clean spring-boot:run
```

Open:

```text
http://localhost:8092
```

---

# Step 16: Test

## Test 1: Publish one event

Click:

```text
Publish Single Event
```

Expected UI:

```text
Produced: 1
Consumed: 0 or 1
Estimated Lag: 1 or 0
```

Because the consumer sleeps for 2 seconds, the UI may briefly show lag.

---

## Test 2: Publish bulk events

Enter:

```text
Number of Orders: 10
```

Click:

```text
Publish Bulk Events
```

Expected immediately:

```text
Produced: 10
Consumed: 0, 1, 2...
Estimated Lag: visible number
```

After some time, page refreshes and consumed count increases.

Eventually:

```text
Produced: 10
Consumed: 10
Estimated Lag: 0
```

---

# Step 17: Kafka Commands for Real Lag

## Describe topic

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic orders-lag-topic
```

## Read topic directly

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic orders-lag-topic \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true \
  --property key.separator=" | "
```

## Check consumer groups

```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --list
```

Expected:

```text
slow-reporting-group
```

## Real Kafka lag command

```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group slow-reporting-group
```

Output style:

```text
GROUP                 TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
slow-reporting-group  orders-lag-topic  0          3               5               2
slow-reporting-group  orders-lag-topic  1          4               4               0
slow-reporting-group  orders-lag-topic  2          2               6               4
```

Meaning:

```text
CURRENT-OFFSET = how far the consumer group has read
LOG-END-OFFSET = latest offset available in Kafka
LAG = unread messages for that consumer group
```

---

# Step 18: Expected Console Output

Producer:

```text
Order Event Published:
Key: ORD-AB123
Value: {"orderId":"ORD-AB123","customerName":"Customer-1","productName":"Product-1","amount":1001.0,"eventType":"ORDER_CREATED","eventTime":"2026-06-23T17:05:10.100"}
--------------------------------
```

Consumer:

```text
Slow Consumer Processed Order:
Order ID: ORD-AB123
Partition: 1
Offset: 4
Consumed Count: 1
--------------------------------
```

---

#  Explanation

```text
In this example, the producer sends order events quickly.

The consumer is intentionally slow because we added Thread.sleep(2000).

Kafka will continue accepting messages into the topic.

The consumer will process them one by one.

The difference between what is available in Kafka and what the consumer has processed is called consumer lag.

Consumer lag helps operations teams identify whether consumers are keeping up with producers.
```

---

# Important Kafka Learning

```text
Consumer lag is not about the producer being broken.

Consumer lag usually means:

1. Producer is sending faster than consumer can process.
2. Consumer has crashed or stopped.
3. Consumer group has too few instances.
4. Consumer processing logic is slow.
5. External dependency is slow, such as database or API.
```

---

# IMPORTANT CLARIFICATION 

```text
The UI value called Estimated Lag is application-level.

The real Kafka lag should be checked using:

kafka-consumer-groups --describe --group slow-reporting-group

Kafka calculates lag using offsets.

Our UI calculates lag using:

producedCount - consumedCount

This is only for easy classroom visualization.
```

---

# Final ReadMe Heading

```text
# UI-12: Consumer Lag Dashboard using Kafka
```

Short description:

```text
This project demonstrates Kafka consumer lag using a Spring Boot Thymeleaf UI.
The producer publishes order events quickly to a Kafka topic.
The consumer intentionally processes messages slowly.
The UI shows produced count, consumed count, estimated lag, and consumed event details.
Real Kafka lag can be verified using kafka-consumer-groups command.
```

---

# What This UI Demonstrates

```text
Kafka topic
Kafka producer
Kafka consumer
Consumer group
Partition
Offset
Slow consumer
Consumer lag
Application-level dashboard
Real Kafka lag verification using CLI
```


---

# 1. Bulk Push Code

This controller method receives the form submit:

```java
@PostMapping("/orders/bulk")
public String publishBulkOrders(@RequestParam int count) {

    orderEventProducer.publishBulkOrders(count);

    return "redirect:/";
}
```

This calls:

```java
orderEventProducer.publishBulkOrders(count);
```

---

# 2. Producer Sends Multiple Messages Quickly

Inside `OrderEventProducer.java`:

```java
public void publishBulkOrders(int count) {

    for (int i = 1; i <= count; i++) {
        publishOrder(
                "Customer-" + i,
                "Product-" + i,
                1000 + i
        );
    }
}
```

So if you enter:

```text
10
```

It does this:

```text
publishOrder(Customer-1, Product-1, 1001)
publishOrder(Customer-2, Product-2, 1002)
publishOrder(Customer-3, Product-3, 1003)
...
publishOrder(Customer-10, Product-10, 1010)
```

Each call sends one Kafka message.

---

# 3. Actual Kafka Send

Inside `publishOrder()`:

```java
kafkaTemplate.send(
        "orders-lag-topic",
        orderId,
        eventJson
);
```

This means:

```text
Topic = orders-lag-topic
Key   = orderId
Value = JSON order event
```

Example:

```json
{
  "orderId": "ORD-A12BC",
  "customerName": "Customer-1",
  "productName": "Product-1",
  "amount": 1001.0,
  "eventType": "ORDER_CREATED",
  "eventTime": "2026-06-23T22:50:10"
}
```

---

# 4. Where Do These Messages Go?

Your topic has 3 partitions:

```java
@Bean
public NewTopic ordersLagTopic() {
    return new NewTopic("orders-lag-topic", 3, (short) 1);
}
```

Think of partitions like buckets:

```text
orders-lag-topic

Partition 0 = bucket 0
Partition 1 = bucket 1
Partition 2 = bucket 2
```

Because we send with `orderId` as key:

```java
kafkaTemplate.send("orders-lag-topic", orderId, eventJson);
```

Kafka decides partition based on the key.

So 10 messages may be distributed like this:

```text
Partition 0:
ORD-A1111
ORD-B2222
ORD-C3333

Partition 1:
ORD-D4444
ORD-E5555
ORD-F6666
ORD-G7777

Partition 2:
ORD-H8888
ORD-I9999
ORD-J0000
```

It may not be exactly equal every time, but Kafka spreads keys across partitions.

---

# 5. How Consumer Consumes It

Your consumer is:

```java
@KafkaListener(
        topics = "orders-lag-topic",
        groupId = "slow-reporting-group"
)
public void consume(ConsumerRecord<String, String> record) throws Exception {

    Thread.sleep(2000);

    OrderEvent orderEvent =
            objectMapper.readValue(record.value(), OrderEvent.class);

    consumedCount.incrementAndGet();
}
```

There is **one consumer instance** in this app.

So Kafka assigns all 3 partitions to this one consumer:

```text
slow-reporting-group

SlowOrderConsumer
    ├── orders-lag-topic-0
    ├── orders-lag-topic-1
    └── orders-lag-topic-2
```

That means this single consumer is responsible for all three buckets.

---

# 6. Why Lag Appears

Producer sends quickly:

```text
Message 1 sent
Message 2 sent
Message 3 sent
Message 4 sent
Message 5 sent
...
Message 10 sent
```

But consumer is slow because we wrote:

```java
Thread.sleep(2000);
```

So consumer processes like this:

```text
Consume message 1 → wait 2 seconds
Consume message 2 → wait 2 seconds
Consume message 3 → wait 2 seconds
Consume message 4 → wait 2 seconds
...
```

So if you push 10 messages quickly, producer finishes almost immediately, but consumer needs roughly:

```text
10 messages × 2 seconds = around 20 seconds
```

That is why the UI shows:

```text
Produced: 10
Consumed: 2
Estimated Lag: 8
```

Then after some time:

```text
Produced: 10
Consumed: 5
Estimated Lag: 5
```

Then finally:

```text
Produced: 10
Consumed: 10
Estimated Lag: 0
```

---

# 7. Simple Visual Explanation

```text
Bulk Button Clicked
        ↓
Producer sends 10 messages quickly
        ↓
orders-lag-topic
        ↓
Partition 0     Partition 1     Partition 2
  Msg 1           Msg 2           Msg 3
  Msg 4           Msg 5           Msg 6
  Msg 7           Msg 8           Msg 9
                                  Msg 10
        ↓
SlowOrderConsumer reads slowly
        ↓
2 seconds per message
        ↓
Lag becomes visible
```

---

# 8. Important Clarification

```text
Kafka lag is real.

But our UI lag number is approximate.

UI lag = producedCount - consumedCount
```

Real Kafka lag is checked using:

```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group slow-reporting-group
```

