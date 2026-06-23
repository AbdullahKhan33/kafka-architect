# UI-10: Inventory Stock Alert using Kafka

## Business Scenario

A retail company receives inventory updates from warehouse systems.

Whenever stock quantity changes, an inventory event is published to Kafka.

If stock becomes less than or equal to the reorder threshold, the application publishes a new alert event to another Kafka topic.

```text
inventory-events-topic
        ↓
InventoryEventConsumer
        ↓
Checks stock quantity
        ↓
If low stock
        ↓
stock-alert-topic
        ↓
StockAlertConsumer
        ↓
UI dashboard
```

---

# Step 1: Create Spring Boot Project

Project name:

```text
inventory-stock-alert-ui
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
src/main/java/com/example/inventory/

├── InventoryStockAlertUiApplication.java

├── config/
│   └── KafkaTopicConfig.java

├── controller/
│   └── InventoryController.java

├── model/
│   ├── InventoryEvent.java
│   ├── StockAlertEvent.java
│   ├── InventoryView.java
│   └── EventActivityLog.java

├── producer/
│   ├── InventoryEventProducer.java
│   └── StockAlertProducer.java

└── consumer/
    ├── InventoryEventConsumer.java
    └── StockAlertConsumer.java

src/main/resources/
├── templates/
│   └── inventory.html
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
    <artifactId>inventory-stock-alert-ui</artifactId>
    <version>0.0.1-SNAPSHOT</version>

    <name>inventory-stock-alert-ui</name>

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

Use this Kafka dependency:

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
spring.application.name=inventory-stock-alert-ui

server.port=8090

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
src/main/java/com/example/inventory/InventoryStockAlertUiApplication.java
```

```java
package com.example.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class InventoryStockAlertUiApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryStockAlertUiApplication.class, args);
    }
}
```

---

# Step 6: KafkaTopicConfig.java

File:

```text
src/main/java/com/example/inventory/config/KafkaTopicConfig.java
```

```java
package com.example.inventory.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic inventoryEventsTopic() {
        return new NewTopic("inventory-events-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic stockAlertTopic() {
        return new NewTopic("stock-alert-topic", 3, (short) 1);
    }
}
```

We are using two topics:

```text
inventory-events-topic = original inventory update events
stock-alert-topic      = derived low-stock alert events
```

---

# Step 7: InventoryEvent.java

File:

```text
src/main/java/com/example/inventory/model/InventoryEvent.java
```

```java
package com.example.inventory.model;

public class InventoryEvent {

    private String eventId;
    private String productId;
    private String productName;
    private int currentStock;
    private int reorderThreshold;
    private String warehouseLocation;
    private String eventType;
    private String eventTime;

    public InventoryEvent() {
    }

    public InventoryEvent(
            String eventId,
            String productId,
            String productName,
            int currentStock,
            int reorderThreshold,
            String warehouseLocation,
            String eventType,
            String eventTime) {

        this.eventId = eventId;
        this.productId = productId;
        this.productName = productName;
        this.currentStock = currentStock;
        this.reorderThreshold = reorderThreshold;
        this.warehouseLocation = warehouseLocation;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public String getEventId() {
        return eventId;
    }

    public String getProductId() {
        return productId;
    }

    public String getProductName() {
        return productName;
    }

    public int getCurrentStock() {
        return currentStock;
    }

    public int getReorderThreshold() {
        return reorderThreshold;
    }

    public String getWarehouseLocation() {
        return warehouseLocation;
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

# Step 8: StockAlertEvent.java

File:

```text
src/main/java/com/example/inventory/model/StockAlertEvent.java
```

```java
package com.example.inventory.model;

public class StockAlertEvent {

    private String alertId;
    private String productId;
    private String productName;
    private int currentStock;
    private int reorderThreshold;
    private String warehouseLocation;
    private String alertMessage;
    private String alertLevel;
    private String eventTime;

    public StockAlertEvent() {
    }

    public StockAlertEvent(
            String alertId,
            String productId,
            String productName,
            int currentStock,
            int reorderThreshold,
            String warehouseLocation,
            String alertMessage,
            String alertLevel,
            String eventTime) {

        this.alertId = alertId;
        this.productId = productId;
        this.productName = productName;
        this.currentStock = currentStock;
        this.reorderThreshold = reorderThreshold;
        this.warehouseLocation = warehouseLocation;
        this.alertMessage = alertMessage;
        this.alertLevel = alertLevel;
        this.eventTime = eventTime;
    }

    public String getAlertId() {
        return alertId;
    }

    public String getProductId() {
        return productId;
    }

    public String getProductName() {
        return productName;
    }

    public int getCurrentStock() {
        return currentStock;
    }

    public int getReorderThreshold() {
        return reorderThreshold;
    }

    public String getWarehouseLocation() {
        return warehouseLocation;
    }

    public String getAlertMessage() {
        return alertMessage;
    }

    public String getAlertLevel() {
        return alertLevel;
    }

    public String getEventTime() {
        return eventTime;
    }
}
```

---

# Step 9: InventoryView.java

This class stores latest stock status for the dashboard.

File:

```text
src/main/java/com/example/inventory/model/InventoryView.java
```

```java
package com.example.inventory.model;

public class InventoryView {

    private String productId;
    private String productName;
    private int currentStock;
    private int reorderThreshold;
    private String warehouseLocation;
    private String stockStatus;
    private String lastUpdatedTime;

    public InventoryView() {
    }

    public InventoryView(
            String productId,
            String productName,
            int currentStock,
            int reorderThreshold,
            String warehouseLocation,
            String stockStatus,
            String lastUpdatedTime) {

        this.productId = productId;
        this.productName = productName;
        this.currentStock = currentStock;
        this.reorderThreshold = reorderThreshold;
        this.warehouseLocation = warehouseLocation;
        this.stockStatus = stockStatus;
        this.lastUpdatedTime = lastUpdatedTime;
    }

    public String getProductId() {
        return productId;
    }

    public String getProductName() {
        return productName;
    }

    public int getCurrentStock() {
        return currentStock;
    }

    public int getReorderThreshold() {
        return reorderThreshold;
    }

    public String getWarehouseLocation() {
        return warehouseLocation;
    }

    public String getStockStatus() {
        return stockStatus;
    }

    public String getLastUpdatedTime() {
        return lastUpdatedTime;
    }
}
```

---

# Step 10: EventActivityLog.java

This class stores event activity for the UI.

## IMPORTANT CLARIFICATION

```text
This is not Kafka’s internal event log.

This is our application-level dashboard log.

Kafka’s actual event log is inside topic partitions.

This Java object is only for showing consumed activity in the browser.
```

File:

```text
src/main/java/com/example/inventory/model/EventActivityLog.java
```

```java
package com.example.inventory.model;

public class EventActivityLog {

    private String eventSource;
    private String eventType;
    private String productId;
    private String productName;
    private String topic;
    private int partition;
    private long offset;
    private String message;

    public EventActivityLog() {
    }

    public EventActivityLog(
            String eventSource,
            String eventType,
            String productId,
            String productName,
            String topic,
            int partition,
            long offset,
            String message) {

        this.eventSource = eventSource;
        this.eventType = eventType;
        this.productId = productId;
        this.productName = productName;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.message = message;
    }

    public String getEventSource() {
        return eventSource;
    }

    public String getEventType() {
        return eventType;
    }

    public String getProductId() {
        return productId;
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

    public String getMessage() {
        return message;
    }
}
```

---

# Step 11: InventoryEventProducer.java

File:

```text
src/main/java/com/example/inventory/producer/InventoryEventProducer.java
```

```java
package com.example.inventory.producer;

import com.example.inventory.model.InventoryEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class InventoryEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public InventoryEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishInventoryUpdate(
            String productId,
            String productName,
            int currentStock,
            int reorderThreshold,
            String warehouseLocation) {

        try {
            String eventId =
                    "INV-" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();

            InventoryEvent inventoryEvent =
                    new InventoryEvent(
                            eventId,
                            productId,
                            productName,
                            currentStock,
                            reorderThreshold,
                            warehouseLocation,
                            "INVENTORY_UPDATED",
                            LocalDateTime.now().toString()
                    );

            String eventJson =
                    objectMapper.writeValueAsString(inventoryEvent);

            kafkaTemplate.send(
                    "inventory-events-topic",
                    productId,
                    eventJson
            );

            System.out.println("Inventory Event Published:");
            System.out.println("Key: " + productId);
            System.out.println("Value: " + eventJson);
            System.out.println("--------------------------------");

        } catch (Exception e) {
            throw new RuntimeException("Error publishing inventory event", e);
        }
    }
}
```

---

# Step 12: StockAlertProducer.java

This producer publishes low-stock alerts to a second topic.

File:

```text
src/main/java/com/example/inventory/producer/StockAlertProducer.java
```

```java
package com.example.inventory.producer;

import com.example.inventory.model.StockAlertEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class StockAlertProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public StockAlertProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishStockAlert(
            String productId,
            String productName,
            int currentStock,
            int reorderThreshold,
            String warehouseLocation) {

        try {
            String alertId =
                    "ALT-" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();

            String alertLevel;

            if (currentStock == 0) {
                alertLevel = "CRITICAL";
            } else {
                alertLevel = "LOW";
            }

            String alertMessage =
                    "Stock is below or equal to reorder threshold. Reorder required.";

            StockAlertEvent stockAlertEvent =
                    new StockAlertEvent(
                            alertId,
                            productId,
                            productName,
                            currentStock,
                            reorderThreshold,
                            warehouseLocation,
                            alertMessage,
                            alertLevel,
                            LocalDateTime.now().toString()
                    );

            String alertJson =
                    objectMapper.writeValueAsString(stockAlertEvent);

            kafkaTemplate.send(
                    "stock-alert-topic",
                    productId,
                    alertJson
            );

            System.out.println("Stock Alert Published:");
            System.out.println("Key: " + productId);
            System.out.println("Value: " + alertJson);
            System.out.println("--------------------------------");

        } catch (Exception e) {
            throw new RuntimeException("Error publishing stock alert event", e);
        }
    }
}
```

---

# Step 13: InventoryEventConsumer.java

This consumer reads inventory events and decides whether an alert should be created.

## IMPORTANT CLARIFICATION

```text
Kafka is not deciding that stock is low.

The Java consumer is checking:

currentStock <= reorderThreshold

If the condition is true, our application publishes a new event to stock-alert-topic.
```

File:

```text
src/main/java/com/example/inventory/consumer/InventoryEventConsumer.java
```

```java
package com.example.inventory.consumer;

import com.example.inventory.model.EventActivityLog;
import com.example.inventory.model.InventoryEvent;
import com.example.inventory.model.InventoryView;
import com.example.inventory.producer.StockAlertProducer;
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
public class InventoryEventConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final StockAlertProducer stockAlertProducer;

    private final Map<String, InventoryView> inventoryViewMap =
            new ConcurrentHashMap<>();

    private final List<EventActivityLog> activityLogs =
            Collections.synchronizedList(new ArrayList<>());

    public InventoryEventConsumer(StockAlertProducer stockAlertProducer) {
        this.stockAlertProducer = stockAlertProducer;
    }

    @KafkaListener(
            topics = "inventory-events-topic",
            groupId = "inventory-monitor-group"
    )
    public void consumeInventoryEvent(ConsumerRecord<String, String> record)
            throws Exception {

        InventoryEvent inventoryEvent =
                objectMapper.readValue(record.value(), InventoryEvent.class);

        String stockStatus;

        if (inventoryEvent.getCurrentStock()
                <= inventoryEvent.getReorderThreshold()) {

            stockStatus = "LOW_STOCK";

            stockAlertProducer.publishStockAlert(
                    inventoryEvent.getProductId(),
                    inventoryEvent.getProductName(),
                    inventoryEvent.getCurrentStock(),
                    inventoryEvent.getReorderThreshold(),
                    inventoryEvent.getWarehouseLocation()
            );

        } else {
            stockStatus = "STOCK_OK";
        }

        InventoryView inventoryView =
                new InventoryView(
                        inventoryEvent.getProductId(),
                        inventoryEvent.getProductName(),
                        inventoryEvent.getCurrentStock(),
                        inventoryEvent.getReorderThreshold(),
                        inventoryEvent.getWarehouseLocation(),
                        stockStatus,
                        inventoryEvent.getEventTime()
                );

        inventoryViewMap.put(
                inventoryEvent.getProductId(),
                inventoryView
        );

        String message =
                "Inventory event consumed. Stock status calculated as "
                        + stockStatus;

        EventActivityLog activityLog =
                new EventActivityLog(
                        "InventoryEventConsumer",
                        inventoryEvent.getEventType(),
                        inventoryEvent.getProductId(),
                        inventoryEvent.getProductName(),
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        message
                );

        activityLogs.add(0, activityLog);

        if (activityLogs.size() > 50) {
            activityLogs.remove(activityLogs.size() - 1);
        }

        System.out.println("Inventory Event Consumed:");
        System.out.println("Product ID: " + inventoryEvent.getProductId());
        System.out.println("Current Stock: " + inventoryEvent.getCurrentStock());
        System.out.println("Threshold: " + inventoryEvent.getReorderThreshold());
        System.out.println("Status: " + stockStatus);
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("--------------------------------");
    }

    public Map<String, InventoryView> getInventoryViewMap() {
        return inventoryViewMap;
    }

    public List<EventActivityLog> getActivityLogs() {
        return activityLogs;
    }

    public void clearInventoryView() {
        inventoryViewMap.clear();
        activityLogs.clear();
    }
}
```

---

# Step 14: StockAlertConsumer.java

This consumer reads from `stock-alert-topic` and displays alerts in the UI.

File:

```text
src/main/java/com/example/inventory/consumer/StockAlertConsumer.java
```

```java
package com.example.inventory.consumer;

import com.example.inventory.model.EventActivityLog;
import com.example.inventory.model.StockAlertEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
public class StockAlertConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final List<StockAlertEvent> stockAlerts =
            Collections.synchronizedList(new ArrayList<>());

    private final List<EventActivityLog> alertActivityLogs =
            Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(
            topics = "stock-alert-topic",
            groupId = "stock-alert-dashboard-group"
    )
    public void consumeStockAlert(ConsumerRecord<String, String> record)
            throws Exception {

        StockAlertEvent stockAlertEvent =
                objectMapper.readValue(record.value(), StockAlertEvent.class);

        stockAlerts.add(0, stockAlertEvent);

        if (stockAlerts.size() > 50) {
            stockAlerts.remove(stockAlerts.size() - 1);
        }

        EventActivityLog activityLog =
                new EventActivityLog(
                        "StockAlertConsumer",
                        "STOCK_ALERT_CREATED",
                        stockAlertEvent.getProductId(),
                        stockAlertEvent.getProductName(),
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        stockAlertEvent.getAlertMessage()
                );

        alertActivityLogs.add(0, activityLog);

        if (alertActivityLogs.size() > 50) {
            alertActivityLogs.remove(alertActivityLogs.size() - 1);
        }

        System.out.println("Stock Alert Consumed:");
        System.out.println("Product ID: " + stockAlertEvent.getProductId());
        System.out.println("Alert Level: " + stockAlertEvent.getAlertLevel());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("--------------------------------");
    }

    public List<StockAlertEvent> getStockAlerts() {
        return stockAlerts;
    }

    public List<EventActivityLog> getAlertActivityLogs() {
        return alertActivityLogs;
    }

    public void clearAlerts() {
        stockAlerts.clear();
        alertActivityLogs.clear();
    }
}
```

---

# Step 15: InventoryController.java

File:

```text
src/main/java/com/example/inventory/controller/InventoryController.java
```

```java
package com.example.inventory.controller;

import com.example.inventory.consumer.InventoryEventConsumer;
import com.example.inventory.consumer.StockAlertConsumer;
import com.example.inventory.producer.InventoryEventProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
public class InventoryController {

    private final InventoryEventProducer inventoryEventProducer;
    private final InventoryEventConsumer inventoryEventConsumer;
    private final StockAlertConsumer stockAlertConsumer;

    public InventoryController(
            InventoryEventProducer inventoryEventProducer,
            InventoryEventConsumer inventoryEventConsumer,
            StockAlertConsumer stockAlertConsumer) {

        this.inventoryEventProducer = inventoryEventProducer;
        this.inventoryEventConsumer = inventoryEventConsumer;
        this.stockAlertConsumer = stockAlertConsumer;
    }

    @GetMapping("/")
    public String showPage(Model model) {
        addDashboardData(model);
        return "inventory";
    }

    @PostMapping("/inventory-events")
    public String publishInventoryEvent(
            @RequestParam String productId,
            @RequestParam String productName,
            @RequestParam int currentStock,
            @RequestParam int reorderThreshold,
            @RequestParam String warehouseLocation,
            Model model) {

        inventoryEventProducer.publishInventoryUpdate(
                productId,
                productName,
                currentStock,
                reorderThreshold,
                warehouseLocation
        );

        waitForConsumers();

        model.addAttribute(
                "message",
                "Inventory event published to Kafka."
        );

        model.addAttribute(
                "productId",
                productId
        );

        addDashboardData(model);

        return "inventory";
    }

    @PostMapping("/clear")
    public String clearDashboard() {

        inventoryEventConsumer.clearInventoryView();
        stockAlertConsumer.clearAlerts();

        return "redirect:/";
    }

    private void addDashboardData(Model model) {

        model.addAttribute(
                "inventory",
                inventoryEventConsumer.getInventoryViewMap()
        );

        model.addAttribute(
                "alerts",
                stockAlertConsumer.getStockAlerts()
        );

        model.addAttribute(
                "inventoryLogs",
                inventoryEventConsumer.getActivityLogs()
        );

        model.addAttribute(
                "alertLogs",
                stockAlertConsumer.getAlertActivityLogs()
        );
    }

    private void waitForConsumers() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
```

---

# Step 16: inventory.html

File:

```text
src/main/resources/templates/inventory.html
```

```html
<!DOCTYPE html>
<html xmlns:th="http://www.thymeleaf.org">
<head>
    <title>Inventory Stock Alert using Kafka</title>

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

        .clarification {
            background: #fff3cd;
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

        .ok {
            font-weight: bold;
            color: #2e7d32;
        }

        .low {
            font-weight: bold;
            color: #c62828;
        }

        .alert {
            font-weight: bold;
            color: #c62828;
        }
    </style>
</head>

<body>

<h1>UI-10: Inventory Stock Alert using Kafka</h1>

<div class="info">
    <h3>Kafka Flow</h3>

    <p>
        Inventory updates are published to <code>inventory-events-topic</code>.
        The consumer checks the stock quantity.
        If stock is low, a new alert event is published to <code>stock-alert-topic</code>.
    </p>

    <pre>
Inventory UI
    ↓
inventory-events-topic
    ↓
InventoryEventConsumer
    ↓
if currentStock <= reorderThreshold
    ↓
stock-alert-topic
    ↓
StockAlertConsumer
    ↓
Dashboard
    </pre>
</div>

<div class="clarification">
    <h3>Important Clarification</h3>

    <p>
        Kafka is responsible for storing and delivering events through topics.
    </p>

    <p>
        Kafka is not deciding whether stock is low.
        The low-stock check is written in Java application code.
    </p>

    <p>
        The dashboard tables are also application-level views.
        They are stored in memory using Java collections.
        Kafka's actual event log is inside topic partitions.
    </p>
</div>

<div th:if="${message}" class="success">
    <h3>Kafka Event Published</h3>
    <p th:text="${message}"></p>
    <p>Product ID: <b th:text="${productId}"></b></p>
</div>

<div class="container">

    <div>

        <div class="card">
            <h2>Publish Inventory Update</h2>

            <form method="post" action="/inventory-events">

                <label>Product ID</label>
                <input type="text" name="productId" value="PROD-101" required>

                <label>Product Name</label>
                <input type="text" name="productName" value="Laptop" required>

                <label>Current Stock</label>
                <input type="number" name="currentStock" value="25" required>

                <label>Reorder Threshold</label>
                <input type="number" name="reorderThreshold" value="10" required>

                <label>Warehouse Location</label>
                <select name="warehouseLocation" required>
                    <option value="Hyderabad Warehouse">Hyderabad Warehouse</option>
                    <option value="Bengaluru Warehouse">Bengaluru Warehouse</option>
                    <option value="Mumbai Warehouse">Mumbai Warehouse</option>
                    <option value="Delhi Warehouse">Delhi Warehouse</option>
                </select>

                <button type="submit">Publish Inventory Event</button>

            </form>
        </div>

        <div class="card">
            <h2>Quick Test Values</h2>

            <pre>
Stock OK:
Current Stock = 25
Threshold     = 10

Low Stock:
Current Stock = 5
Threshold     = 10

Critical:
Current Stock = 0
Threshold     = 10
            </pre>
        </div>

        <div class="card">
            <h2>Clear Dashboard</h2>

            <form method="post" action="/clear">
                <button class="danger" type="submit">Clear Application View</button>
            </form>
        </div>

    </div>

    <div class="wide-card">

        <h2>Current Inventory Status - Application View</h2>

        <table>
            <thead>
            <tr>
                <th>Product ID</th>
                <th>Product</th>
                <th>Stock</th>
                <th>Threshold</th>
                <th>Warehouse</th>
                <th>Status</th>
                <th>Last Updated</th>
            </tr>
            </thead>

            <tbody>
            <tr th:each="entry : ${inventory}">
                <td th:text="${entry.value.productId}"></td>
                <td th:text="${entry.value.productName}"></td>
                <td th:text="${entry.value.currentStock}"></td>
                <td th:text="${entry.value.reorderThreshold}"></td>
                <td th:text="${entry.value.warehouseLocation}"></td>

                <td th:if="${entry.value.stockStatus == 'STOCK_OK'}"
                    class="ok"
                    th:text="${entry.value.stockStatus}">
                </td>

                <td th:if="${entry.value.stockStatus == 'LOW_STOCK'}"
                    class="low"
                    th:text="${entry.value.stockStatus}">
                </td>

                <td th:text="${entry.value.lastUpdatedTime}"></td>
            </tr>
            </tbody>
        </table>

        <h2>Stock Alerts - Application View</h2>

        <table>
            <thead>
            <tr>
                <th>Alert ID</th>
                <th>Product ID</th>
                <th>Product</th>
                <th>Stock</th>
                <th>Threshold</th>
                <th>Alert Level</th>
                <th>Message</th>
                <th>Time</th>
            </tr>
            </thead>

            <tbody>
            <tr th:each="alert : ${alerts}">
                <td th:text="${alert.alertId}"></td>
                <td th:text="${alert.productId}"></td>
                <td th:text="${alert.productName}"></td>
                <td th:text="${alert.currentStock}"></td>
                <td th:text="${alert.reorderThreshold}"></td>
                <td class="alert" th:text="${alert.alertLevel}"></td>
                <td th:text="${alert.alertMessage}"></td>
                <td th:text="${alert.eventTime}"></td>
            </tr>
            </tbody>
        </table>

        <h2>Consumed Inventory Events - Application View</h2>

        <p>
            This table is maintained by the Spring Boot application.
            Kafka stores the actual event log inside topic partitions.
        </p>

        <table>
            <thead>
            <tr>
                <th>Consumer</th>
                <th>Event Type</th>
                <th>Product ID</th>
                <th>Product</th>
                <th>Topic</th>
                <th>Partition</th>
                <th>Offset</th>
                <th>Message</th>
            </tr>
            </thead>

            <tbody>
            <tr th:each="log : ${inventoryLogs}">
                <td th:text="${log.eventSource}"></td>
                <td th:text="${log.eventType}"></td>
                <td th:text="${log.productId}"></td>
                <td th:text="${log.productName}"></td>
                <td th:text="${log.topic}"></td>
                <td th:text="${log.partition}"></td>
                <td th:text="${log.offset}"></td>
                <td th:text="${log.message}"></td>
            </tr>
            </tbody>
        </table>

        <h2>Consumed Stock Alert Events - Application View</h2>

        <table>
            <thead>
            <tr>
                <th>Consumer</th>
                <th>Event Type</th>
                <th>Product ID</th>
                <th>Product</th>
                <th>Topic</th>
                <th>Partition</th>
                <th>Offset</th>
                <th>Message</th>
            </tr>
            </thead>

            <tbody>
            <tr th:each="log : ${alertLogs}">
                <td th:text="${log.eventSource}"></td>
                <td th:text="${log.eventType}"></td>
                <td th:text="${log.productId}"></td>
                <td th:text="${log.productName}"></td>
                <td th:text="${log.topic}"></td>
                <td th:text="${log.partition}"></td>
                <td th:text="${log.offset}"></td>
                <td th:text="${log.message}"></td>
            </tr>
            </tbody>
        </table>

    </div>

</div>

</body>
</html>
```

---

# Step 17: Start Kafka

If Kafka and Zookeeper containers are already created, run:

```bash
docker start zookeeper
sleep 15
docker start kafka
sleep 15
docker ps
```

Verify topics:

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --list
```

After the Spring Boot app starts, you should see:

```text
inventory-events-topic
stock-alert-topic
```

---

# Step 18: Run App

Run main class:

```text
InventoryStockAlertUiApplication
```

Or from terminal:

```bash
mvn clean spring-boot:run
```

Open:

```text
http://localhost:8090
```

---

# Step 19: Test

## Test 1: Stock OK

Enter:

```text
Product ID: PROD-101
Product Name: Laptop
Current Stock: 25
Reorder Threshold: 10
Warehouse Location: Hyderabad Warehouse
```

Click:

```text
Publish Inventory Event
```

Expected current inventory status:

```text
PROD-101    Laptop    25    10    Hyderabad Warehouse    STOCK_OK
```

Expected stock alerts table:

```text
No alert generated
```

Expected topic flow:

```text
inventory-events-topic received the inventory event.
stock-alert-topic does not receive anything because stock is OK.
```

---

## Test 2: Low Stock

Enter:

```text
Product ID: PROD-101
Product Name: Laptop
Current Stock: 5
Reorder Threshold: 10
Warehouse Location: Hyderabad Warehouse
```

Click:

```text
Publish Inventory Event
```

Expected current inventory status:

```text
PROD-101    Laptop    5    10    Hyderabad Warehouse    LOW_STOCK
```

Expected stock alert:

```text
PROD-101    Laptop    5    10    LOW
```

Expected topic flow:

```text
inventory-events-topic receives the inventory event.

InventoryEventConsumer checks:
5 <= 10

So it publishes a new alert event to:
stock-alert-topic
```

---

## Test 3: Critical Stock

Enter:

```text
Product ID: PROD-101
Product Name: Laptop
Current Stock: 0
Reorder Threshold: 10
Warehouse Location: Hyderabad Warehouse
```

Expected stock alert:

```text
Alert Level: CRITICAL
```

---

# Step 20: Expected Console Output

Producer output:

```text
Inventory Event Published:
Key: PROD-101
Value: {"eventId":"INV-A91BC","productId":"PROD-101","productName":"Laptop","currentStock":5,"reorderThreshold":10,"warehouseLocation":"Hyderabad Warehouse","eventType":"INVENTORY_UPDATED","eventTime":"2026-06-23T14:10:12.559"}
--------------------------------
```

Inventory consumer output:

```text
Inventory Event Consumed:
Product ID: PROD-101
Current Stock: 5
Threshold: 10
Status: LOW_STOCK
Partition: 2
Offset: 4
--------------------------------
```

Alert producer output:

```text
Stock Alert Published:
Key: PROD-101
Value: {"alertId":"ALT-B12DE","productId":"PROD-101","productName":"Laptop","currentStock":5,"reorderThreshold":10,"warehouseLocation":"Hyderabad Warehouse","alertMessage":"Stock is below or equal to reorder threshold. Reorder required.","alertLevel":"LOW","eventTime":"2026-06-23T14:10:13.010"}
--------------------------------
```

Alert consumer output:

```text
Stock Alert Consumed:
Product ID: PROD-101
Alert Level: LOW
Partition: 2
Offset: 1
--------------------------------
```

---

# Step 21: Kafka Commands for Extensive Verification

## List topics

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --list
```

## Describe inventory topic

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic inventory-events-topic
```

## Describe alert topic

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic stock-alert-topic
```

## Read inventory events from Kafka topic

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic inventory-events-topic \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true \
  --property key.separator=" | "
```

## Read stock alert events from Kafka topic

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic stock-alert-topic \
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

Expected groups:

```text
inventory-monitor-group
stock-alert-dashboard-group
```

## Describe inventory consumer group

```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group inventory-monitor-group
```

## Describe stock alert consumer group

```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group stock-alert-dashboard-group
```

---

# Explanation

```text
In this example, the inventory update is the first event.

The user enters product stock details from the UI.

The producer publishes the event to inventory-events-topic.

The InventoryEventConsumer reads that topic.

Then the Java code checks whether currentStock is less than or equal to reorderThreshold.

If the condition is true, the application publishes a second event to stock-alert-topic.

So this example shows a topic-to-topic event flow.

Kafka is carrying the events.

The stock check is not a Kafka feature. It is application logic written inside the consumer.
```

---

# Important Kafka Learning

```text
Kafka is not only used for producer-to-consumer messaging.

A consumer can also act as a processor.

It can read from one topic, apply business logic, and publish a new event to another topic.
```

Architecture:

```text
Input topic
    ↓
Consumer / Processor
    ↓
Output topic
```

In our example:

```text
inventory-events-topic
    ↓
InventoryEventConsumer
    ↓
stock-alert-topic
```

---

# Important Clarification for Participants

```text
Do not confuse application dashboard data with Kafka topic data.

The dashboard tables are maintained by Java objects like ArrayList and Map.

If the Spring Boot app restarts, the dashboard view is cleared.

But Kafka may still have the original events inside the topic, depending on retention.

To see the actual Kafka event log, use kafka-console-consumer.
```
