
# UI-13: Order Fulfillment Workflow using Kafka Event Choreography

## Business Scenario

An e-commerce order should move through this sequence:

```text
ORDER_PLACED
    ↓
PAYMENT_RECEIVED
    ↓
ORDER_PACKED
    ↓
ORDER_SHIPPED
    ↓
DELIVERED
```

Each service listens to one topic and publishes the next event.

```text
Order UI
   ↓
order-events-topic
   ↓
PaymentConsumer
   ↓
payment-events-topic
   ↓
PackingConsumer
   ↓
packing-events-topic
   ↓
ShippingConsumer
   ↓
shipping-events-topic
   ↓
DeliveryConsumer
   ↓
delivery-events-topic
```

## IMPORTANT CLARIFICATION

```text
Kafka feature:
Topic, producer, consumer, consumer group, partition, offset, message key.

Application feature:
Business sequencing, payment-before-packing rule, order trace dashboard, HashMap trace store.
```

Kafka does not execute the workflow. Kafka carries the events. The application services create the sequence by listening to one event and publishing the next event.

---

# Step 1: Create Spring Boot Project

Project name:

```text
order-fulfillment-workflow-ui
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
src/main/java/com/example/orderworkflow/

├── OrderFulfillmentWorkflowUiApplication.java

├── config/
│   └── KafkaTopicConfig.java

├── controller/
│   └── WorkflowController.java

├── model/
│   ├── OrderWorkflowEvent.java
│   ├── OrderTraceEvent.java
│   └── OrderStatusView.java

├── producer/
│   └── WorkflowEventProducer.java

├── consumer/
│   ├── PaymentConsumer.java
│   ├── PackingConsumer.java
│   ├── ShippingConsumer.java
│   ├── DeliveryConsumer.java
│   └── FinalDeliveryConsumer.java

└── service/
    └── OrderTraceService.java

src/main/resources/
├── templates/
│   └── workflow.html
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
    <artifactId>order-fulfillment-workflow-ui</artifactId>
    <version>0.0.1-SNAPSHOT</version>

    <name>order-fulfillment-workflow-ui</name>

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

Important: use this:

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
spring.application.name=order-fulfillment-workflow-ui

server.port=8093

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
src/main/java/com/example/orderworkflow/OrderFulfillmentWorkflowUiApplication.java
```

```java
package com.example.orderworkflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrderFulfillmentWorkflowUiApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderFulfillmentWorkflowUiApplication.class, args);
    }
}
```

---

# Step 6: KafkaTopicConfig.java

File:

```text
src/main/java/com/example/orderworkflow/config/KafkaTopicConfig.java
```

```java
package com.example.orderworkflow.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderEventsTopic() {
        return new NewTopic("order-events-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic paymentEventsTopic() {
        return new NewTopic("payment-events-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic packingEventsTopic() {
        return new NewTopic("packing-events-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic shippingEventsTopic() {
        return new NewTopic("shipping-events-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic deliveryEventsTopic() {
        return new NewTopic("delivery-events-topic", 3, (short) 1);
    }
}
```

We are using separate topics to make sequencing very clear.

```text
PackingConsumer does not listen to order-events-topic.
PackingConsumer listens only to payment-events-topic.

So packing starts only after PAYMENT_RECEIVED.
```

---

# Step 7: OrderWorkflowEvent.java

File:

```text
src/main/java/com/example/orderworkflow/model/OrderWorkflowEvent.java
```

```java
package com.example.orderworkflow.model;

public class OrderWorkflowEvent {

    private String eventId;
    private String orderId;
    private String correlationId;
    private String customerName;
    private String productName;
    private double amount;
    private String eventType;
    private String status;
    private String sourceService;
    private String eventTime;

    public OrderWorkflowEvent() {
    }

    public OrderWorkflowEvent(
            String eventId,
            String orderId,
            String correlationId,
            String customerName,
            String productName,
            double amount,
            String eventType,
            String status,
            String sourceService,
            String eventTime) {

        this.eventId = eventId;
        this.orderId = orderId;
        this.correlationId = correlationId;
        this.customerName = customerName;
        this.productName = productName;
        this.amount = amount;
        this.eventType = eventType;
        this.status = status;
        this.sourceService = sourceService;
        this.eventTime = eventTime;
    }

    public String getEventId() {
        return eventId;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCorrelationId() {
        return correlationId;
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

    public String getStatus() {
        return status;
    }

    public String getSourceService() {
        return sourceService;
    }

    public String getEventTime() {
        return eventTime;
    }
}
```

---

# Step 8: OrderTraceEvent.java

This is used for tracing one order across the workflow.

## IMPORTANT CLARIFICATION

```text
This is not Kafka internal storage.

This is our application-level trace view.

In production, this should go to a database, Elasticsearch, OpenSearch, or Azure AI Search.
```

File:

```text
src/main/java/com/example/orderworkflow/model/OrderTraceEvent.java
```

```java
package com.example.orderworkflow.model;

public class OrderTraceEvent {

    private String orderId;
    private String correlationId;
    private String eventType;
    private String status;
    private String sourceService;
    private String topic;
    private int partition;
    private long offset;
    private String eventTime;

    public OrderTraceEvent() {
    }

    public OrderTraceEvent(
            String orderId,
            String correlationId,
            String eventType,
            String status,
            String sourceService,
            String topic,
            int partition,
            long offset,
            String eventTime) {

        this.orderId = orderId;
        this.correlationId = correlationId;
        this.eventType = eventType;
        this.status = status;
        this.sourceService = sourceService;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.eventTime = eventTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getStatus() {
        return status;
    }

    public String getSourceService() {
        return sourceService;
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

# Step 9: OrderStatusView.java

This stores the latest order status for the dashboard.

File:

```text
src/main/java/com/example/orderworkflow/model/OrderStatusView.java
```

```java
package com.example.orderworkflow.model;

public class OrderStatusView {

    private String orderId;
    private String correlationId;
    private String customerName;
    private String productName;
    private double amount;
    private String currentStatus;
    private String lastUpdatedBy;
    private String lastUpdatedTime;

    public OrderStatusView() {
    }

    public OrderStatusView(
            String orderId,
            String correlationId,
            String customerName,
            String productName,
            double amount,
            String currentStatus,
            String lastUpdatedBy,
            String lastUpdatedTime) {

        this.orderId = orderId;
        this.correlationId = correlationId;
        this.customerName = customerName;
        this.productName = productName;
        this.amount = amount;
        this.currentStatus = currentStatus;
        this.lastUpdatedBy = lastUpdatedBy;
        this.lastUpdatedTime = lastUpdatedTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCorrelationId() {
        return correlationId;
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

    public String getCurrentStatus() {
        return currentStatus;
    }

    public String getLastUpdatedBy() {
        return lastUpdatedBy;
    }

    public String getLastUpdatedTime() {
        return lastUpdatedTime;
    }
}
```

---

# Step 10: OrderTraceService.java

File:

```text
src/main/java/com/example/orderworkflow/service/OrderTraceService.java
```

```java
package com.example.orderworkflow.service;

import com.example.orderworkflow.model.OrderStatusView;
import com.example.orderworkflow.model.OrderTraceEvent;
import com.example.orderworkflow.model.OrderWorkflowEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class OrderTraceService {

    private final Map<String, List<OrderTraceEvent>> traceStore =
            new ConcurrentHashMap<>();

    private final Map<String, OrderStatusView> orderStatusMap =
            new ConcurrentHashMap<>();

    private final List<OrderTraceEvent> allEvents =
            Collections.synchronizedList(new ArrayList<>());

    public void addTrace(
            OrderWorkflowEvent event,
            ConsumerRecord<String, String> record,
            String serviceName) {

        OrderTraceEvent traceEvent =
                new OrderTraceEvent(
                        event.getOrderId(),
                        event.getCorrelationId(),
                        event.getEventType(),
                        event.getStatus(),
                        serviceName,
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        event.getEventTime()
                );

        traceStore
                .computeIfAbsent(
                        event.getOrderId(),
                        key -> Collections.synchronizedList(new ArrayList<>())
                )
                .add(traceEvent);

        allEvents.add(0, traceEvent);

        if (allEvents.size() > 100) {
            allEvents.remove(allEvents.size() - 1);
        }

        OrderStatusView statusView =
                new OrderStatusView(
                        event.getOrderId(),
                        event.getCorrelationId(),
                        event.getCustomerName(),
                        event.getProductName(),
                        event.getAmount(),
                        event.getStatus(),
                        serviceName,
                        event.getEventTime()
                );

        orderStatusMap.put(event.getOrderId(), statusView);
    }

    public List<OrderTraceEvent> getTraceByOrderId(String orderId) {

        List<OrderTraceEvent> events =
                traceStore.getOrDefault(
                        orderId,
                        Collections.emptyList()
                );

        List<OrderTraceEvent> sortedEvents =
                new ArrayList<>(events);

        sortedEvents.sort(
                Comparator.comparing(OrderTraceEvent::getEventTime)
        );

        return sortedEvents;
    }

    public Map<String, OrderStatusView> getOrderStatusMap() {
        return orderStatusMap;
    }

    public List<OrderTraceEvent> getAllEvents() {
        return allEvents;
    }

    public void clear() {
        traceStore.clear();
        orderStatusMap.clear();
        allEvents.clear();
    }
}
```

## Important explanation

```text
traceStore is only a demo trace store.

Real production version:
Kafka events
   ↓
Trace Consumer
   ↓
Database / Elasticsearch / Azure AI Search
   ↓
Trace UI
```

---

# Step 11: WorkflowEventProducer.java

File:

```text
src/main/java/com/example/orderworkflow/producer/WorkflowEventProducer.java
```

```java
package com.example.orderworkflow.producer;

import com.example.orderworkflow.model.OrderWorkflowEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class WorkflowEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public WorkflowEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String placeOrder(
            String customerName,
            String productName,
            double amount) {

        String orderId =
                "ORD-" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();

        String correlationId = orderId;

        publishEvent(
                "order-events-topic",
                orderId,
                correlationId,
                customerName,
                productName,
                amount,
                "ORDER_PLACED",
                "ORDER_PLACED",
                "OrderService"
        );

        return orderId;
    }

    public void publishPaymentReceived(OrderWorkflowEvent previousEvent) {

        publishEvent(
                "payment-events-topic",
                previousEvent.getOrderId(),
                previousEvent.getCorrelationId(),
                previousEvent.getCustomerName(),
                previousEvent.getProductName(),
                previousEvent.getAmount(),
                "PAYMENT_RECEIVED",
                "PAYMENT_RECEIVED",
                "PaymentService"
        );
    }

    public void publishOrderPacked(OrderWorkflowEvent previousEvent) {

        publishEvent(
                "packing-events-topic",
                previousEvent.getOrderId(),
                previousEvent.getCorrelationId(),
                previousEvent.getCustomerName(),
                previousEvent.getProductName(),
                previousEvent.getAmount(),
                "ORDER_PACKED",
                "ORDER_PACKED",
                "PackingService"
        );
    }

    public void publishOrderShipped(OrderWorkflowEvent previousEvent) {

        publishEvent(
                "shipping-events-topic",
                previousEvent.getOrderId(),
                previousEvent.getCorrelationId(),
                previousEvent.getCustomerName(),
                previousEvent.getProductName(),
                previousEvent.getAmount(),
                "ORDER_SHIPPED",
                "ORDER_SHIPPED",
                "ShippingService"
        );
    }

    public void publishDelivered(OrderWorkflowEvent previousEvent) {

        publishEvent(
                "delivery-events-topic",
                previousEvent.getOrderId(),
                previousEvent.getCorrelationId(),
                previousEvent.getCustomerName(),
                previousEvent.getProductName(),
                previousEvent.getAmount(),
                "DELIVERED",
                "DELIVERED",
                "DeliveryService"
        );
    }

    private void publishEvent(
            String topic,
            String orderId,
            String correlationId,
            String customerName,
            String productName,
            double amount,
            String eventType,
            String status,
            String sourceService) {

        try {
            String eventId =
                    "EVT-" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();

            OrderWorkflowEvent event =
                    new OrderWorkflowEvent(
                            eventId,
                            orderId,
                            correlationId,
                            customerName,
                            productName,
                            amount,
                            eventType,
                            status,
                            sourceService,
                            LocalDateTime.now().toString()
                    );

            String eventJson =
                    objectMapper.writeValueAsString(event);

            kafkaTemplate.send(
                    topic,
                    orderId,
                    eventJson
            );

            System.out.println("Workflow Event Published:");
            System.out.println("Topic: " + topic);
            System.out.println("Key: " + orderId);
            System.out.println("Event Type: " + eventType);
            System.out.println("Value: " + eventJson);
            System.out.println("--------------------------------");

        } catch (Exception e) {
            throw new RuntimeException("Error publishing workflow event", e);
        }
    }
}
```

## Important point

```java
kafkaTemplate.send(topic, orderId, eventJson);
```

We are using `orderId` as the Kafka message key.

That helps all events for the same order go consistently to a partition within a topic.

---

# Step 12: PaymentConsumer.java

File:

```text
src/main/java/com/example/orderworkflow/consumer/PaymentConsumer.java
```

```java
package com.example.orderworkflow.consumer;

import com.example.orderworkflow.model.OrderWorkflowEvent;
import com.example.orderworkflow.producer.WorkflowEventProducer;
import com.example.orderworkflow.service.OrderTraceService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PaymentConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final WorkflowEventProducer workflowEventProducer;
    private final OrderTraceService orderTraceService;

    public PaymentConsumer(
            WorkflowEventProducer workflowEventProducer,
            OrderTraceService orderTraceService) {

        this.workflowEventProducer = workflowEventProducer;
        this.orderTraceService = orderTraceService;
    }

    @KafkaListener(
            topics = "order-events-topic",
            groupId = "payment-service-group"
    )
    public void consume(ConsumerRecord<String, String> record)
            throws Exception {

        OrderWorkflowEvent event =
                objectMapper.readValue(record.value(), OrderWorkflowEvent.class);

        orderTraceService.addTrace(
                event,
                record,
                "PaymentConsumer"
        );

        Thread.sleep(700);

        workflowEventProducer.publishPaymentReceived(event);

        System.out.println("PaymentConsumer processed ORDER_PLACED:");
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("--------------------------------");
    }
}
```

## Teaching point

```text
PaymentConsumer listens to order-events-topic.

After it receives ORDER_PLACED, it publishes PAYMENT_RECEIVED.
```

---

# Step 13: PackingConsumer.java

File:

```text
src/main/java/com/example/orderworkflow/consumer/PackingConsumer.java
```

```java
package com.example.orderworkflow.consumer;

import com.example.orderworkflow.model.OrderWorkflowEvent;
import com.example.orderworkflow.producer.WorkflowEventProducer;
import com.example.orderworkflow.service.OrderTraceService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PackingConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final WorkflowEventProducer workflowEventProducer;
    private final OrderTraceService orderTraceService;

    public PackingConsumer(
            WorkflowEventProducer workflowEventProducer,
            OrderTraceService orderTraceService) {

        this.workflowEventProducer = workflowEventProducer;
        this.orderTraceService = orderTraceService;
    }

    @KafkaListener(
            topics = "payment-events-topic",
            groupId = "packing-service-group"
    )
    public void consume(ConsumerRecord<String, String> record)
            throws Exception {

        OrderWorkflowEvent event =
                objectMapper.readValue(record.value(), OrderWorkflowEvent.class);

        orderTraceService.addTrace(
                event,
                record,
                "PackingConsumer"
        );

        Thread.sleep(700);

        workflowEventProducer.publishOrderPacked(event);

        System.out.println("PackingConsumer processed PAYMENT_RECEIVED:");
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("--------------------------------");
    }
}
```

## Teaching point

```text
PackingConsumer does not listen to ORDER_PLACED.

PackingConsumer listens only to payment-events-topic.

So packing happens only after payment is received.
```

---

# Step 14: ShippingConsumer.java

File:

```text
src/main/java/com/example/orderworkflow/consumer/ShippingConsumer.java
```

```java
package com.example.orderworkflow.consumer;

import com.example.orderworkflow.model.OrderWorkflowEvent;
import com.example.orderworkflow.producer.WorkflowEventProducer;
import com.example.orderworkflow.service.OrderTraceService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ShippingConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final WorkflowEventProducer workflowEventProducer;
    private final OrderTraceService orderTraceService;

    public ShippingConsumer(
            WorkflowEventProducer workflowEventProducer,
            OrderTraceService orderTraceService) {

        this.workflowEventProducer = workflowEventProducer;
        this.orderTraceService = orderTraceService;
    }

    @KafkaListener(
            topics = "packing-events-topic",
            groupId = "shipping-service-group"
    )
    public void consume(ConsumerRecord<String, String> record)
            throws Exception {

        OrderWorkflowEvent event =
                objectMapper.readValue(record.value(), OrderWorkflowEvent.class);

        orderTraceService.addTrace(
                event,
                record,
                "ShippingConsumer"
        );

        Thread.sleep(700);

        workflowEventProducer.publishOrderShipped(event);

        System.out.println("ShippingConsumer processed ORDER_PACKED:");
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("--------------------------------");
    }
}
```

---

# Step 15: DeliveryConsumer.java

File:

```text
src/main/java/com/example/orderworkflow/consumer/DeliveryConsumer.java
```

```java
package com.example.orderworkflow.consumer;

import com.example.orderworkflow.model.OrderWorkflowEvent;
import com.example.orderworkflow.producer.WorkflowEventProducer;
import com.example.orderworkflow.service.OrderTraceService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class DeliveryConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final WorkflowEventProducer workflowEventProducer;
    private final OrderTraceService orderTraceService;

    public DeliveryConsumer(
            WorkflowEventProducer workflowEventProducer,
            OrderTraceService orderTraceService) {

        this.workflowEventProducer = workflowEventProducer;
        this.orderTraceService = orderTraceService;
    }

    @KafkaListener(
            topics = "shipping-events-topic",
            groupId = "delivery-service-group"
    )
    public void consume(ConsumerRecord<String, String> record)
            throws Exception {

        OrderWorkflowEvent event =
                objectMapper.readValue(record.value(), OrderWorkflowEvent.class);

        orderTraceService.addTrace(
                event,
                record,
                "DeliveryConsumer"
        );

        Thread.sleep(700);

        workflowEventProducer.publishDelivered(event);

        System.out.println("DeliveryConsumer processed ORDER_SHIPPED:");
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("--------------------------------");
    }
}
```

---

# Step 16: FinalDeliveryConsumer.java

This final consumer reads `delivery-events-topic` and records the final delivered event in the trace dashboard.

File:

```text
src/main/java/com/example/orderworkflow/consumer/FinalDeliveryConsumer.java
```

```java
package com.example.orderworkflow.consumer;

import com.example.orderworkflow.model.OrderWorkflowEvent;
import com.example.orderworkflow.service.OrderTraceService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class FinalDeliveryConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OrderTraceService orderTraceService;

    public FinalDeliveryConsumer(OrderTraceService orderTraceService) {
        this.orderTraceService = orderTraceService;
    }

    @KafkaListener(
            topics = "delivery-events-topic",
            groupId = "final-dashboard-group"
    )
    public void consume(ConsumerRecord<String, String> record)
            throws Exception {

        OrderWorkflowEvent event =
                objectMapper.readValue(record.value(), OrderWorkflowEvent.class);

        orderTraceService.addTrace(
                event,
                record,
                "FinalDeliveryConsumer"
        );

        System.out.println("FinalDeliveryConsumer recorded DELIVERED:");
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("--------------------------------");
    }
}
```

---

# Step 17: WorkflowController.java

File:

```text
src/main/java/com/example/orderworkflow/controller/WorkflowController.java
```

```java
package com.example.orderworkflow.controller;

import com.example.orderworkflow.producer.WorkflowEventProducer;
import com.example.orderworkflow.service.OrderTraceService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
public class WorkflowController {

    private final WorkflowEventProducer workflowEventProducer;
    private final OrderTraceService orderTraceService;

    public WorkflowController(
            WorkflowEventProducer workflowEventProducer,
            OrderTraceService orderTraceService) {

        this.workflowEventProducer = workflowEventProducer;
        this.orderTraceService = orderTraceService;
    }

    @GetMapping("/")
    public String showPage(Model model) {

        addDashboardData(model);

        return "workflow";
    }

    @PostMapping("/orders")
    public String placeOrder(
            @RequestParam String customerName,
            @RequestParam String productName,
            @RequestParam double amount) {

        workflowEventProducer.placeOrder(
                customerName,
                productName,
                amount
        );

        return "redirect:/";
    }

    @GetMapping("/trace")
    public String traceOrder(
            @RequestParam String orderId,
            Model model) {

        addDashboardData(model);

        model.addAttribute(
                "searchedOrderId",
                orderId
        );

        model.addAttribute(
                "selectedTrace",
                orderTraceService.getTraceByOrderId(orderId)
        );

        return "workflow";
    }

    @PostMapping("/clear")
    public String clear() {

        orderTraceService.clear();

        return "redirect:/";
    }

    private void addDashboardData(Model model) {

        model.addAttribute(
                "orders",
                orderTraceService.getOrderStatusMap()
        );

        model.addAttribute(
                "allEvents",
                orderTraceService.getAllEvents()
        );
    }
}
```

Notice we use `redirect:/` after POST. This avoids the same auto-refresh/POST issue from the lag dashboard.

---

# Step 18: workflow.html

File:

```text
src/main/resources/templates/workflow.html
```

```html
<!DOCTYPE html>
<html xmlns:th="http://www.thymeleaf.org">
<head>
    <title>Order Fulfillment Workflow using Kafka</title>

    <meta http-equiv="refresh" content="4">

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

        .trace-box {
            background: #e8f5e9;
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

        .status {
            font-weight: bold;
            color: #1565c0;
        }

        .event {
            font-weight: bold;
            color: #6a1b9a;
        }
    </style>
</head>

<body>

<h1>UI-13: Order Fulfillment Workflow using Kafka Event Choreography</h1>

<div class="info">
    <h3>Kafka Event Choreography Flow</h3>

    <p>
        Each service listens to one topic and publishes the next event.
        This creates sequencing without one central controller.
    </p>

    <pre>
ORDER_PLACED
    ↓
PaymentConsumer
    ↓
PAYMENT_RECEIVED
    ↓
PackingConsumer
    ↓
ORDER_PACKED
    ↓
ShippingConsumer
    ↓
ORDER_SHIPPED
    ↓
DeliveryConsumer
    ↓
DELIVERED
    </pre>
</div>

<div class="clarification">
    <h3>Important Clarification</h3>

    <p>
        Kafka transports and stores events.
        Kafka does not decide that payment must happen before packing.
    </p>

    <p>
        The sequence is created by application design.
        PackingConsumer listens only to payment-events-topic.
        Therefore, packing starts only after PAYMENT_RECEIVED exists.
    </p>

    <p>
        The trace dashboard is also an application-level view.
        In production, this trace data should go to a database, Elasticsearch, OpenSearch, or Azure AI Search.
    </p>
</div>

<div class="container">

    <div>

        <div class="card">
            <h2>Place Order</h2>

            <form method="post" action="/orders">

                <label>Customer Name</label>
                <input type="text" name="customerName" value="Abdullah" required>

                <label>Product Name</label>
                <input type="text" name="productName" value="Laptop" required>

                <label>Amount</label>
                <input type="number" step="0.01" name="amount" value="65000" required>

                <button type="submit">Place Order</button>

            </form>
        </div>

        <div class="card">
            <h2>Trace One Order</h2>

            <form method="get" action="/trace">

                <label>Order ID</label>
                <input type="text" name="orderId" placeholder="Example: ORD-ABCDE" required>

                <button type="submit">Trace Order</button>

            </form>
        </div>

        <div class="card">
            <h2>Clear Application View</h2>

            <form method="post" action="/clear">
                <button class="danger" type="submit">Clear Dashboard</button>
            </form>
        </div>

        <div class="card">
            <h2>Real Production Trace Store</h2>

            <pre>
Kafka Topics
    ↓
Trace Consumer
    ↓
Database
    ↓
Elasticsearch / Azure Search
    ↓
Trace UI
            </pre>
        </div>

    </div>

    <div class="wide-card">

        <h2>Current Order Status - Application View</h2>

        <table>
            <thead>
            <tr>
                <th>Order ID</th>
                <th>Correlation ID</th>
                <th>Customer</th>
                <th>Product</th>
                <th>Amount</th>
                <th>Current Status</th>
                <th>Last Updated By</th>
                <th>Last Updated Time</th>
            </tr>
            </thead>

            <tbody>
            <tr th:each="entry : ${orders}">
                <td th:text="${entry.value.orderId}"></td>
                <td th:text="${entry.value.correlationId}"></td>
                <td th:text="${entry.value.customerName}"></td>
                <td th:text="${entry.value.productName}"></td>
                <td th:text="${entry.value.amount}"></td>
                <td class="status" th:text="${entry.value.currentStatus}"></td>
                <td th:text="${entry.value.lastUpdatedBy}"></td>
                <td th:text="${entry.value.lastUpdatedTime}"></td>
            </tr>
            </tbody>
        </table>

        <div th:if="${searchedOrderId}" class="trace-box">
            <h2>Trace Result for Order: <span th:text="${searchedOrderId}"></span></h2>

            <p>
                This timeline is fetched from the application trace store using orderId.
                In production, this search would happen from a database or search index.
            </p>

            <table>
                <thead>
                <tr>
                    <th>Event Type</th>
                    <th>Status</th>
                    <th>Service</th>
                    <th>Topic</th>
                    <th>Partition</th>
                    <th>Offset</th>
                    <th>Event Time</th>
                </tr>
                </thead>

                <tbody>
                <tr th:each="trace : ${selectedTrace}">
                    <td class="event" th:text="${trace.eventType}"></td>
                    <td class="status" th:text="${trace.status}"></td>
                    <td th:text="${trace.sourceService}"></td>
                    <td th:text="${trace.topic}"></td>
                    <td th:text="${trace.partition}"></td>
                    <td th:text="${trace.offset}"></td>
                    <td th:text="${trace.eventTime}"></td>
                </tr>
                </tbody>
            </table>
        </div>

        <h2>All Workflow Events - Application View</h2>

        <p>
            This table is maintained by the Spring Boot application in memory.
            Kafka stores the actual event log inside topic partitions.
        </p>

        <table>
            <thead>
            <tr>
                <th>Order ID</th>
                <th>Correlation ID</th>
                <th>Event Type</th>
                <th>Status</th>
                <th>Service</th>
                <th>Topic</th>
                <th>Partition</th>
                <th>Offset</th>
                <th>Event Time</th>
            </tr>
            </thead>

            <tbody>
            <tr th:each="event : ${allEvents}">
                <td th:text="${event.orderId}"></td>
                <td th:text="${event.correlationId}"></td>
                <td class="event" th:text="${event.eventType}"></td>
                <td class="status" th:text="${event.status}"></td>
                <td th:text="${event.sourceService}"></td>
                <td th:text="${event.topic}"></td>
                <td th:text="${event.partition}"></td>
                <td th:text="${event.offset}"></td>
                <td th:text="${event.eventTime}"></td>
            </tr>
            </tbody>
        </table>

    </div>

</div>

</body>
</html>
```

---

# Step 19: Start Kafka

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

After app startup, expected topics:

```text
order-events-topic
payment-events-topic
packing-events-topic
shipping-events-topic
delivery-events-topic
```

---

# Step 20: Run App

Run main class:

```text
OrderFulfillmentWorkflowUiApplication
```

Or:

```bash
mvn clean spring-boot:run
```

Open:

```text
http://localhost:8093
```

---

# Step 21: Test

## Test 1: Place one order

Enter:

```text
Customer Name: Abdullah
Product Name: Laptop
Amount: 65000
```

Click:

```text
Place Order
```

The page refreshes every 4 seconds. You should see the order move through:

```text
ORDER_PLACED
PAYMENT_RECEIVED
ORDER_PACKED
ORDER_SHIPPED
DELIVERED
```

Expected current status:

```text
ORD-XXXXX    DELIVERED
```

---

## Test 2: Trace one order

Copy the generated order ID from the current order table.

Search:

```text
ORD-XXXXX
```

Expected trace timeline:

```text
ORDER_PLACED        order-events-topic       PaymentConsumer
PAYMENT_RECEIVED    payment-events-topic     PackingConsumer
ORDER_PACKED        packing-events-topic     ShippingConsumer
ORDER_SHIPPED       shipping-events-topic    DeliveryConsumer
DELIVERED           delivery-events-topic    FinalDeliveryConsumer
```

This answers the participant question:

```text
Out of millions of orders, how do we trace one order?
```

Answer:

```text
Every event carries orderId and correlationId.

A trace consumer/view stores searchable metadata.

The UI searches by orderId or correlationId.

In production, the trace store is a database, Elasticsearch, OpenSearch, or Azure AI Search.
```

---

# Step 22: Kafka Commands for Verification

## Describe all topics

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic order-events-topic
```

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic payment-events-topic
```

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic packing-events-topic
```

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic shipping-events-topic
```

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic delivery-events-topic
```

## Read order events

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic order-events-topic \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true \
  --property key.separator=" | "
```

## Read payment events

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic payment-events-topic \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true \
  --property key.separator=" | "
```

## Read packing events

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic packing-events-topic \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true \
  --property key.separator=" | "
```

## Read shipping events

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic shipping-events-topic \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true \
  --property key.separator=" | "
```

## Read delivery events

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic delivery-events-topic \
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
payment-service-group
packing-service-group
shipping-service-group
delivery-service-group
final-dashboard-group
```

## Describe one group

```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group payment-service-group
```

---

# Step 23: Expected Console Output

```text
Workflow Event Published:
Topic: order-events-topic
Key: ORD-8A91B
Event Type: ORDER_PLACED
--------------------------------
```

```text
PaymentConsumer processed ORDER_PLACED:
Order ID: ORD-8A91B
--------------------------------
```

```text
Workflow Event Published:
Topic: payment-events-topic
Key: ORD-8A91B
Event Type: PAYMENT_RECEIVED
--------------------------------
```

```text
PackingConsumer processed PAYMENT_RECEIVED:
Order ID: ORD-8A91B
--------------------------------
```

```text
Workflow Event Published:
Topic: packing-events-topic
Key: ORD-8A91B
Event Type: ORDER_PACKED
--------------------------------
```

```text
ShippingConsumer processed ORDER_PACKED:
Order ID: ORD-8A91B
--------------------------------
```

```text
Workflow Event Published:
Topic: shipping-events-topic
Key: ORD-8A91B
Event Type: ORDER_SHIPPED
--------------------------------
```

```text
DeliveryConsumer processed ORDER_SHIPPED:
Order ID: ORD-8A91B
--------------------------------
```

```text
Workflow Event Published:
Topic: delivery-events-topic
Key: ORD-8A91B
Event Type: DELIVERED
--------------------------------
```

---
