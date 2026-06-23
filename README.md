# Kafka Architect - UI Based Kafka Projects

This repository contains a series of simple, practical, UI-based Apache Kafka projects built using Spring Boot, Spring Kafka, and Thymeleaf.

Each module demonstrates one Kafka concept through a small real-world business scenario.

The goal of this repository is to help participants understand Kafka concepts visually using browser-based applications instead of only command-line producers and consumers.

---

## Repository Structure

```text
1.Setup
module02
module03
module04
module05
module06
module07
module08
module09
module10
module11
````

---

## Modules

| Module   | Project / Concept   | Description                                                                 |
| -------- | ------------------- | --------------------------------------------------------------------------- |
| 1.Setup  | Environment Setup   | Kafka, Zookeeper, Java, Maven, and project setup instructions               |
| module02 | Hospital System     | Basic Kafka producer-consumer example using a hospital domain               |
| module03 | Order System        | Order event publishing and consumption                                      |
| module04 | Stock Trading       | Stock trade event processing                                                |
| module05 | Transactions        | Transaction event processing using Kafka                                    |
| module06 | Payment DLQ         | Payment processing with Dead Letter Queue / Dead Letter Topic concept       |
| module07 | Portfolio Dashboard | Kafka Streams example for live portfolio aggregation                        |
| module08 | Delivery Status     | Delivery status tracking using multiple Kafka events                        |
| module09 | Inventory           | Inventory stock alert using topic-to-topic Kafka processing                 |
| module10 | Consumer Lag        | Consumer lag dashboard using a slow consumer                                |
| module11 | Order Fulfillment   | Order fulfillment workflow using Kafka event choreography and order tracing |

---

## Technology Stack

```text
Java
Spring Boot
Spring Web
Spring for Apache Kafka
Thymeleaf
Apache Kafka
Docker
Maven
```

---

## Common Dependencies

Most projects use the following Spring Boot dependencies:

```text
Spring Web
Spring for Apache Kafka
Thymeleaf
Lombok
```

Important Kafka dependency:

```xml
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
</dependency>
```

Do not use:

```text
spring-boot-starter-kafka
```

---

## How to Start Kafka

If Kafka and Zookeeper containers are already created, start them using:

```bash
docker start zookeeper
sleep 15
docker start kafka
sleep 15
docker ps
```

Verify Kafka topics:

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --list
```

---

## How to Run a Module

Go inside any module folder:

```bash
cd module11
```

Run the Spring Boot application:

```bash
mvn clean spring-boot:run
```

Open the browser using the port mentioned in that module's `application.properties`.

Example:

```text
http://localhost:8093
```

---

## Kafka Verification Commands

### List Topics

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --list
```

### Describe a Topic

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic <topic-name>
```

Example:

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic order-events-topic
```

### Read Messages from a Topic

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic <topic-name> \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true \
  --property key.separator=" | "
```

Example:

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

### List Consumer Groups

```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --list
```

### Describe Consumer Group

```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group <group-name>
```

Example:

```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group payment-service-group
```

---

## Important Kafka Concepts Covered

```text
Kafka Topic
Producer
Consumer
Consumer Group
Partition
Offset
Message Key
Consumer Lag
Dead Letter Topic
Kafka Streams
Topic-to-topic processing
Event choreography
Order tracing
Correlation ID
```

---

## Important Clarification

The browser dashboards in these projects are application-level views.

For example:

```text
ArrayList
HashMap
ConcurrentHashMap
Thymeleaf tables
```

These are not Kafka internal storage.

Kafka stores the actual event log inside topic partitions.

The UI tables are created by the Spring Boot application to help visualize consumed events.

In production systems, these application views are usually replaced with:

```text
Database
Elasticsearch
OpenSearch
Azure AI Search
Observability platform
```

---

## Module 11 Highlight: Order Fulfillment Workflow

`module11` demonstrates event choreography and order tracing.

Business flow:

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

Kafka flow:

```text
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

Tracing flow:

```text
Every event carries orderId and correlationId.

Each consumer records:
orderId
correlationId
eventType
status
serviceName
topic
partition
offset
eventTime
```

In this demo, tracing is stored in memory.

In production, this trace data should be stored in:

```text
Database
Elasticsearch
OpenSearch
Azure AI Search
```

---

## Learning Goal

By the end of these modules, participants should be able to explain:

```text
How Kafka producers publish events
How consumers read from topics
How consumer groups work
How partitions and offsets help Kafka scale
How lag is created and monitored
How retry and DLQ patterns are designed
How events can drive business workflows
How one order can be traced across multiple Kafka topics
```

---

```text
Database persistence
Security
Authentication
Authorization
Monitoring
Retry policies
Error handling
Schema registry
Observability
CI/CD
Cloud deployment
```

---

## Author

Abdullah Khan

