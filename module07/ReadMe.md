
# UI6: Live Portfolio Dashboard using Kafka Streams

## Step 1: Create Spring Boot Project

Project name:

```text
portfolio-dashboard-ui
```

Dependencies:

```text
Spring Web
Spring for Apache Kafka
Thymeleaf
Lombok
```

Also add Kafka Streams dependency manually in `pom.xml`.

---

# Step 2: Project Structure

```text
src/main/java/com/example/portfolio/

├── PortfolioDashboardUiApplication.java
├── controller/
│   └── TradeController.java
├── model/
│   └── TradeEvent.java
├── producer/
│   └── TradeProducer.java
├── consumer/
│   └── DashboardConsumer.java
├── streams/
│   └── PortfolioStreamProcessor.java
└── config/
    └── KafkaTopicConfig.java

src/main/resources/
├── templates/
│   └── trade.html
└── application.properties
```

---

# Step 3: Add Kafka Streams Dependency

In `pom.xml`, inside `<dependencies>`:

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams</artifactId>
</dependency>
```

---

# Step 4: application.properties

```properties
spring.application.name=portfolio-dashboard-ui

server.port=8086

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.consumer.auto-offset-reset=earliest

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

---

# Step 5: Main Class

File:

```text
PortfolioDashboardUiApplication.java
```

```java
package com.example.portfolio;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PortfolioDashboardUiApplication {

    public static void main(String[] args) {
        SpringApplication.run(PortfolioDashboardUiApplication.class, args);
    }

}
```

---

# Step 6: KafkaTopicConfig.java

Package:

```text
com.example.portfolio.config
```

```java
package com.example.portfolio.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic tradesTopic() {
        return new NewTopic("portfolio-trades-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic portfolioSummaryTopic() {
        return new NewTopic("portfolio-summary-topic", 3, (short) 1);
    }
}
```

---

# Step 7: TradeEvent.java

Package:

```text
com.example.portfolio.model
```

```java
package com.example.portfolio.model;

public class TradeEvent {

    private String tradeId;
    private String stockSymbol;
    private int quantity;
    private String eventType;

    public TradeEvent() {
    }

    public TradeEvent(String tradeId, String stockSymbol, int quantity, String eventType) {
        this.tradeId = tradeId;
        this.stockSymbol = stockSymbol;
        this.quantity = quantity;
        this.eventType = eventType;
    }

    public String getTradeId() {
        return tradeId;
    }

    public String getStockSymbol() {
        return stockSymbol;
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

# Step 8: TradeProducer.java

Package:

```text
com.example.portfolio.producer
```

```java
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
```

---

# Step 9: PortfolioStreamProcessor.java

Package:

```text
com.example.portfolio.streams
```

```java
package com.example.portfolio.streams;

import com.example.portfolio.model.TradeEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;

@Component
@EnableKafkaStreams
public class PortfolioStreamProcessor {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Bean
    public KStream<String, String> processTrades(StreamsBuilder streamsBuilder) {

        KStream<String, String> tradesStream =
                streamsBuilder.stream("portfolio-trades-topic");

        KTable<String, Integer> portfolioTable =
                tradesStream
                        .groupByKey(
                                Grouped.with(
                                        Serdes.String(),
                                        Serdes.String()
                                )
                        )
                        .aggregate(
                                () -> 0,
                                (stockSymbol, tradeJson, currentTotal) -> {
                                    try {
                                        TradeEvent tradeEvent =
                                                objectMapper.readValue(
                                                        tradeJson,
                                                        TradeEvent.class
                                                );

                                        return currentTotal + tradeEvent.getQuantity();

                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                },
                                Materialized.with(
                                        Serdes.String(),
                                        Serdes.Integer()
                                )
                        );

        portfolioTable
                .toStream()
                .mapValues(String::valueOf)
                .to(
                        "portfolio-summary-topic",
                        Produced.with(
                                Serdes.String(),
                                Serdes.String()
                        )
                );

        return tradesStream;
    }
}
```

---

# Step 10: DashboardConsumer.java

Package:

```text
com.example.portfolio.consumer
```

```java
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
```

---

# Step 11: TradeController.java

Package:

```text
com.example.portfolio.controller
```

```java
package com.example.portfolio.controller;

import com.example.portfolio.consumer.DashboardConsumer;
import com.example.portfolio.model.TradeEvent;
import com.example.portfolio.producer.TradeProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Controller
public class TradeController {

    private final TradeProducer tradeProducer;
    private final DashboardConsumer dashboardConsumer;

    public TradeController(
            TradeProducer tradeProducer,
            DashboardConsumer dashboardConsumer) {

        this.tradeProducer = tradeProducer;
        this.dashboardConsumer = dashboardConsumer;
    }

    @GetMapping("/")
    public String showPage(Model model) {
        model.addAttribute(
                "portfolio",
                dashboardConsumer.getPortfolioSummary()
        );

        return "trade";
    }

    @PostMapping("/trades")
    public String submitTrade(
            @RequestParam String stockSymbol,
            @RequestParam int quantity,
            Model model) {

        String tradeId =
                "TRD-" + UUID.randomUUID().toString().substring(0, 5);

        TradeEvent tradeEvent =
                new TradeEvent(
                        tradeId,
                        stockSymbol,
                        quantity,
                        "TRADE_SUBMITTED"
                );

        tradeProducer.sendTrade(tradeEvent);

        model.addAttribute("message", "Trade submitted to Kafka Streams.");
        model.addAttribute("tradeId", tradeId);

        try {
            Thread.sleep(800);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        model.addAttribute(
                "portfolio",
                dashboardConsumer.getPortfolioSummary()
        );

        return "trade";
    }
}
```

---

# Step 12: trade.html

File:

```text
src/main/resources/templates/trade.html
```

```html
<!DOCTYPE html>
<html xmlns:th="http://www.thymeleaf.org">
<head>
    <title>Live Portfolio Dashboard</title>

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

        select, input {
            width: 100%;
            padding: 12px;
            margin-top: 12px;
            box-sizing: border-box;
        }

        button {
            width: 100%;
            margin-top: 20px;
            padding: 12px;
            background: #00897b;
            color: white;
            border: none;
            cursor: pointer;
            font-size: 16px;
            border-radius: 5px;
        }

        button:hover {
            background: #00695c;
        }

        .success {
            margin-top: 20px;
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
        }

        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }

        th, td {
            padding: 12px;
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

<h1>Live Portfolio Dashboard using Kafka Streams</h1>

<div class="container">

    <div class="card">

        <h2>Submit Trade</h2>

        <form method="post" action="/trades">

            <label>Stock Symbol</label>
            <select name="stockSymbol" required>
                <option value="">Select Stock</option>
                <option value="AAPL">AAPL - Apple</option>
                <option value="MSFT">MSFT - Microsoft</option>
                <option value="GOOG">GOOG - Google</option>
                <option value="TSLA">TSLA - Tesla</option>
                <option value="AMZN">AMZN - Amazon</option>
            </select>

            <label>Quantity</label>
            <input type="number" name="quantity" placeholder="Quantity" required>

            <button type="submit">Submit Trade</button>

        </form>

        <div class="success" th:if="${message}">
            <h3>Trade Submitted</h3>
            <p th:text="${message}"></p>
            <p>Trade ID: <b th:text="${tradeId}"></b></p>
        </div>

    </div>

    <div class="card">

        <h2>Live Portfolio Summary</h2>

        <table>
            <thead>
            <tr>
                <th>Stock</th>
                <th>Total Quantity</th>
            </tr>
            </thead>

            <tbody>
            <tr th:each="entry : ${portfolio}">
                <td th:text="${entry.key}"></td>
                <td th:text="${entry.value}"></td>
            </tr>
            </tbody>
        </table>

        <div class="info">
            <h3>Kafka Streams Concept</h3>

            <p><b>Input Topic:</b> <code>portfolio-trades-topic</code></p>
            <p><b>Output Topic:</b> <code>portfolio-summary-topic</code></p>

            <pre>
trades-topic
    ↓
Kafka Streams
    ↓
groupByKey()
    ↓
aggregate()
    ↓
portfolio-summary-topic
            </pre>

            <p>
                Kafka Streams continuously calculates the total quantity
                per stock symbol.
            </p>
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
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:9092
```

---

# Step 14: Run App

Run:

```text
PortfolioDashboardUiApplication
```

Open:

```text
http://localhost:8086
```

---

# Step 15: Test

Submit:

```text
AAPL 100
```

Expected table:

```text
AAPL 100
```

Submit:

```text
MSFT 50
```

Expected table:

```text
AAPL 100
MSFT 50
```

Submit:

```text
AAPL 25
```

Expected table:

```text
AAPL 125
MSFT 50
```

Console:

```text
Trade Event Published:
Key: AAPL

Dashboard Updated:
AAPL = 125
```

This UI demonstrates the heart of Kafka Streams: **continuous aggregation from one topic into another topic**.
