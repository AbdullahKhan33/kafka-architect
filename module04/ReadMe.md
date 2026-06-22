You’re right — let’s do UI 3 fully, same style as UI 1 and UI 2.

# UI 3: Stock Trading Partition Dashboard

## Goal

Build a Spring Boot UI where:

```text
User submits stock trade
        ↓
Spring Boot publishes event to Kafka
        ↓
Kafka uses stock symbol as key
        ↓
Same stock symbol goes to same partition
        ↓
Consumer prints partition number
```

This teaches:

```text
Partitions
Message keys
Key-based routing
Ordering for same key
Consumer group processing
```

---

# Step 1: Create New Spring Boot Project

Project name:

```text
stock-trading-partition-ui
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
src/main/java/com/example/trading/

├── StockTradingPartitionUiApplication.java
├── controller/
│   └── TradeController.java
├── model/
│   └── TradeEvent.java
├── producer/
│   └── TradeProducer.java
├── consumer/
│   └── TradeConsumer.java
└── config/
    └── KafkaTopicConfig.java

src/main/resources/
├── templates/
│   └── trade.html
└── application.properties
```

---

# Step 3: application.properties

```properties
spring.application.name=stock-trading-partition-ui

server.port=8083

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.consumer.auto-offset-reset=earliest

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

---

# Pom.xml

```
<dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
</dependency>
```

# Step 4: Main Class

```java
package com.example.trading;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class StockTradingPartitionUiApplication {

    public static void main(String[] args) {
        SpringApplication.run(StockTradingPartitionUiApplication.class, args);
    }

}
```

---

# Step 5: KafkaTopicConfig.java

Package:

```text
com.example.trading.config
```

```java
package com.example.trading.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic tradesTopic() {
        return new NewTopic("trades-topic", 3, (short) 1);
    }
}
```

---

# Step 6: TradeEvent.java

Package:

```text
com.example.trading.model
```

```java
package com.example.trading.model;

public class TradeEvent {

    private String tradeId;
    private String stockSymbol;
    private String tradeType;
    private int quantity;
    private double price;
    private String eventType;

    public TradeEvent() {
    }

    public TradeEvent(String tradeId, String stockSymbol, String tradeType, int quantity, double price, String eventType) {
        this.tradeId = tradeId;
        this.stockSymbol = stockSymbol;
        this.tradeType = tradeType;
        this.quantity = quantity;
        this.price = price;
        this.eventType = eventType;
    }

    public String getTradeId() {
        return tradeId;
    }

    public String getStockSymbol() {
        return stockSymbol;
    }

    public String getTradeType() {
        return tradeType;
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

# Step 7: TradeProducer.java

Package:

```text
com.example.trading.producer
```

```java
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
```

---

# Step 8: TradeController.java

Package:

```text
com.example.trading.controller
```

```java
package com.example.trading.controller;

import com.example.trading.model.TradeEvent;
import com.example.trading.producer.TradeProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Controller
public class TradeController {

    private final TradeProducer tradeProducer;

    public TradeController(TradeProducer tradeProducer) {
        this.tradeProducer = tradeProducer;
    }

    @GetMapping("/")
    public String showTradePage() {
        return "trade";
    }

    @PostMapping("/trades")
    public String placeTrade(
            @RequestParam String stockSymbol,
            @RequestParam String tradeType,
            @RequestParam int quantity,
            @RequestParam double price,
            Model model) {

        String tradeId = "TRD-" + UUID.randomUUID().toString().substring(0, 5);

        TradeEvent tradeEvent = new TradeEvent(
                tradeId,
                stockSymbol.toUpperCase(),
                tradeType,
                quantity,
                price,
                "TRADE_PLACED"
        );

        tradeProducer.sendTradeEvent(tradeEvent);

        model.addAttribute("message", "Trade event published successfully.");
        model.addAttribute("tradeId", tradeId);
        model.addAttribute("stockSymbol", stockSymbol.toUpperCase());

        return "trade";
    }
}
```

---

# Step 9: TradeConsumer.java

Package:

```text
com.example.trading.consumer
```

```java
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
```

---

# Step 10: trade.html

File:

```text
src/main/resources/templates/trade.html
```

```html
<!DOCTYPE html>
<html xmlns:th="http://www.thymeleaf.org">
<head>
    <title>Stock Trading Partition Dashboard</title>

    <style>

        body{
            font-family: Arial;
            background-color:#f4f6f8;
            margin:40px;
        }

        h1{
            color:#333;
        }

        .container{
            display:flex;
            gap:40px;
        }

        .card{
            width:450px;
            background:white;
            padding:25px;
            border-radius:10px;
            box-shadow:0 2px 8px rgba(0,0,0,.1);
        }

        input,select{
            width:100%;
            padding:12px;
            margin-top:15px;
            box-sizing:border-box;
        }

        button{
            width:100%;
            margin-top:20px;
            padding:12px;
            background:#6a1b9a;
            color:white;
            border:none;
            cursor:pointer;
            font-size:16px;
            border-radius:5px;
        }

        button:hover{
            background:#4a148c;
        }

        .success{
            margin-top:20px;
            background:#e8f5e9;
            padding:15px;
            border-radius:8px;
        }

        .info{
            background:#ede7f6;
            padding:20px;
            border-radius:8px;
            line-height:1.8;
        }

        .info code{
            background:#f5f5f5;
            padding:3px 8px;
        }

    </style>

</head>
<body>

<h1>Stock Trading Partition Dashboard</h1>

<div class="container">

    <div class="card">

        <h2>Place Trade</h2>

        <form method="post" action="/trades">

            <label>Stock Symbol</label>

            <select name="stockSymbol" required>
                <option value="">Select Stock Symbol</option>
                <option value="AAPL">AAPL - Apple</option>
                <option value="MSFT">MSFT - Microsoft</option>
                <option value="GOOG">GOOG - Google</option>
                <option value="AMZN">AMZN - Amazon</option>
                <option value="TSLA">TSLA - Tesla</option>
                <option value="NFLX">NFLX - Netflix</option>
                <option value="META">META - Meta</option>
                <option value="NVDA">NVDA - Nvidia</option>
            </select>

            <label>Trade Type</label>

            <select name="tradeType" required>
                <option value="">Select Trade Type</option>
                <option value="BUY">BUY</option>
                <option value="SELL">SELL</option>
            </select>

            <label>Quantity</label>

            <input
                    type="number"
                    name="quantity"
                    placeholder="Quantity"
                    required>

            <label>Price</label>

            <input
                    type="number"
                    step="0.01"
                    name="price"
                    placeholder="Price"
                    required>

            <button type="submit">
                Submit Trade
            </button>

        </form>

        <div class="success" th:if="${message}">

            <h3>Trade Submitted Successfully</h3>

            <p>
                Trade ID :
                <b th:text="${tradeId}"></b>
            </p>

            <p>
                Stock Symbol :
                <b th:text="${stockSymbol}"></b>
            </p>

            <p th:text="${message}"></p>

        </div>

    </div>


    <div class="card">

        <h2>Kafka Partition Concepts</h2>

        <div class="info">

            <p>
                <b>Topic :</b>
                <code>trades-topic</code>
            </p>

            <p>
                <b>Partitions :</b>
                3
            </p>

            <p>
                <b>Kafka Key :</b>
                Stock Symbol
            </p>

            <p>
                Same stock symbol always goes to the same partition.
            </p>

            <hr>

            <h3>Example</h3>

            <pre>
AAPL BUY
AAPL SELL
AAPL BUY

↓ Same Key

Partition 1


MSFT BUY
MSFT SELL

↓ Same Key

Partition 2


TSLA BUY
TSLA SELL

↓ Same Key

Partition 0
            </pre>

            <hr>

            <h3>Partition Diagram</h3>

            <pre>

                trades-topic

        +-----------------------------+

        Partition 0
        TSLA BUY
        TSLA SELL

        -------------------------------

        Partition 1
        AAPL BUY
        AAPL SELL
        AAPL BUY

        -------------------------------

        Partition 2
        MSFT BUY
        MSFT SELL

        +-----------------------------+

            </pre>

            <hr>

            <h3>Key Learning</h3>

            <ul>

                <li>Same key → Same partition</li>

                <li>Ordering guaranteed inside a partition</li>

                <li>Different keys may go to different partitions</li>

                <li>Partitions provide parallel processing</li>

                <li>Consumer groups enable scalability</li>

            </ul>

        </div>

    </div>

</div>

</body>
</html>
```

---

# Step 11: Start Kafka

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

# Step 12: Delete Old Topic If Needed

If `trades-topic` already exists with wrong partitions, delete it:

```bash
kafka-topics.sh \
--delete \
--topic trades-topic \
--bootstrap-server localhost:9092
```

Then start the Spring Boot app again, or manually create:

```bash
kafka-topics.sh \
--create \
--topic trades-topic \
--partitions 3 \
--bootstrap-server localhost:9092
```

Verify:

```bash
kafka-topics.sh \
--describe \
--topic trades-topic \
--bootstrap-server localhost:9092
```

Expected:

```text
PartitionCount: 3
Partition: 0
Partition: 1
Partition: 2
```

---

# Step 13: Run Application

Run:

```text
StockTradingPartitionUiApplication
```

Open:

```text
http://localhost:8083
```

Submit these trades one by one:

```text
AAPL BUY 10 190
AAPL SELL 5 195
AAPL BUY 15 198
MSFT BUY 20 420
MSFT SELL 10 425
TSLA BUY 3 250
TSLA SELL 2 255
```

---

# Expected Console Output

You should see output like:

```text
Published Trade Event
Key: AAPL
Value: {"tradeId":"TRD-a1234","stockSymbol":"AAPL","tradeType":"BUY","quantity":10,"price":190.0,"eventType":"TRADE_PLACED"}

Trade Consumer Received Event
Topic: trades-topic
Partition: 1
Offset: 0
Key: AAPL
Value: {"tradeId":"TRD-a1234","stockSymbol":"AAPL","tradeType":"BUY","quantity":10,"price":190.0,"eventType":"TRADE_PLACED"}
----------------------------------
```

For repeated AAPL trades:

```text
Key: AAPL
Partition: 1

Key: AAPL
Partition: 1

Key: AAPL
Partition: 1
```

MSFT may go to another partition:

```text
Key: MSFT
Partition: 2
```

TSLA may go to another partition:

```text
Key: TSLA
Partition: 0
```

The exact partition numbers can differ, but the **same key should consistently go to the same partition**.

---

# Step 14: Teaching Explanation

```text
Kafka topic = trades-topic

Partitions:
P0
P1
P2

Key = stock symbol
```

Example:

```text
AAPL → P1
AAPL → P1
AAPL → P1

MSFT → P2
MSFT → P2

TSLA → P0
TSLA → P0
```

Visual:

```text
                 trades-topic

        P0              P1              P2
   ------------    ------------    ------------
   TSLA BUY        AAPL BUY        MSFT BUY
   TSLA SELL       AAPL SELL       MSFT SELL
                   AAPL BUY
```

---

# Key Learning

```text
Without key:
Kafka may distribute messages freely.

With key:
Kafka hashes the key.

Same key goes to same partition.

Ordering is guaranteed only inside a partition.

Therefore same customer / account / stock symbol should use the same key.
```

---

# Step 15: Classroom Experiment

Ask participants to send:

```text
AAPL BUY 1 190
AAPL BUY 2 191
AAPL BUY 3 192
AAPL BUY 4 193
```

Then observe:

```text
Same Key: AAPL
Same Partition: same number
Offsets increasing in order
```

Then send:

```text
GOOG BUY 1 140
NFLX SELL 2 500
INFY BUY 5 1500
```

Observe different keys may go to different partitions.

This UI teaches **partitions properly**.
