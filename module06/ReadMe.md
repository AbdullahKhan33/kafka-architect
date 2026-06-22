 Build **UI5: Payment Fraud Detection with Retry + DLQ**.

# UI5: Payment Fraud Detection DLQ

## Goal

```text
User submits payment
        ↓
Payment event goes to payments-topic
        ↓
FraudDetectionConsumer processes it
        ↓
If amount <= 10000 → approved-payments-topic
        ↓
If amount > 10000 → retry → fail → payments-dlt
```

# Step 1: Create Spring Boot Project

Project name:

```text
payment-fraud-dlq-ui
```

Dependencies:

```text
Spring Web
Spring for Apache Kafka
Thymeleaf
Lombok
```

# Step 2: Project Structure

```text
src/main/java/com/example/payment/

├── PaymentFraudDlqUiApplication.java
├── controller/
│   └── PaymentController.java
├── model/
│   └── PaymentEvent.java
├── producer/
│   └── PaymentProducer.java
├── consumer/
│   ├── FraudDetectionConsumer.java
│   ├── ApprovedPaymentConsumer.java
│   └── DltConsumer.java
└── config/
    ├── KafkaTopicConfig.java
    └── KafkaErrorHandlerConfig.java

src/main/resources/
├── templates/
│   └── payment.html
└── application.properties
```

# Step 3: application.properties

```properties
spring.application.name=payment-fraud-dlq-ui

server.port=8085

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.consumer.auto-offset-reset=earliest

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

# Step 4: Main Class

```java
package com.example.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PaymentFraudDlqUiApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentFraudDlqUiApplication.class, args);
    }

}
```

# Step 5: KafkaTopicConfig.java

Package:

```text
com.example.payment.config
```

```java
package com.example.payment.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic paymentsTopic() {
        return new NewTopic("payments-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic approvedPaymentsTopic() {
        return new NewTopic("approved-payments-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic paymentsDltTopic() {
        return new NewTopic("payments-dlt", 3, (short) 1);
    }
}
```

# Step 6: PaymentEvent.java

Package:

```text
com.example.payment.model
```

```java
package com.example.payment.model;

public class PaymentEvent {

    private String paymentId;
    private String customerName;
    private double amount;
    private String cardNumber;
    private String status;
    private String eventType;

    public PaymentEvent() {
    }

    public PaymentEvent(String paymentId, String customerName, double amount, String cardNumber, String status, String eventType) {
        this.paymentId = paymentId;
        this.customerName = customerName;
        this.amount = amount;
        this.cardNumber = cardNumber;
        this.status = status;
        this.eventType = eventType;
    }

    public String getPaymentId() {
        return paymentId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public double getAmount() {
        return amount;
    }

    public String getCardNumber() {
        return cardNumber;
    }

    public String getStatus() {
        return status;
    }

    public String getEventType() {
        return eventType;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
}
```

# Step 7: PaymentProducer.java

Package:

```text
com.example.payment.producer
```

```java
package com.example.payment.producer;

import com.example.payment.model.PaymentEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PaymentProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPayment(PaymentEvent paymentEvent) {
        try {
            String eventJson = objectMapper.writeValueAsString(paymentEvent);

            kafkaTemplate.send(
                    "payments-topic",
                    paymentEvent.getPaymentId(),
                    eventJson
            );

            System.out.println("Payment Event Published:");
            System.out.println(eventJson);

        } catch (Exception e) {
            throw new RuntimeException("Error publishing payment event", e);
        }
    }
}
```

# Step 8: KafkaErrorHandlerConfig.java

Package:

```text
com.example.payment.config
```

```java
package com.example.payment.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;
import org.apache.kafka.common.TopicPartition;

@Configuration
public class KafkaErrorHandlerConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (ConsumerRecord<?, ?> record, Exception exception) ->
                                new TopicPartition("payments-dlt", record.partition())
                );

        FixedBackOff fixedBackOff = new FixedBackOff(2000L, 3);

        return new DefaultErrorHandler(recoverer, fixedBackOff);
    }
}
```

Meaning:

```text
Retry every 2 seconds
Retry 3 times
If still failing, send to payments-dlt
```

# Step 9: FraudDetectionConsumer.java

Package:

```text
com.example.payment.consumer
```

```java
package com.example.payment.consumer;

import com.example.payment.model.PaymentEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class FraudDetectionConsumer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public FraudDetectionConsumer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "payments-topic", groupId = "fraud-detection-group")
    public void consume(String event) {

        try {
            PaymentEvent paymentEvent = objectMapper.readValue(event, PaymentEvent.class);

            System.out.println("Fraud Detection Consumer Received:");
            System.out.println(event);

            if (paymentEvent.getAmount() > 10000) {
                System.out.println("Fraud check failed. Amount too high: " + paymentEvent.getAmount());
                throw new RuntimeException("Suspicious payment detected. Amount exceeds 10000.");
            }

            paymentEvent.setStatus("APPROVED");
            paymentEvent.setEventType("PAYMENT_APPROVED");

            String approvedJson = objectMapper.writeValueAsString(paymentEvent);

            kafkaTemplate.send(
                    "approved-payments-topic",
                    paymentEvent.getPaymentId(),
                    approvedJson
            );

            System.out.println("Payment Approved and Published:");
            System.out.println(approvedJson);

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error processing payment event", e);
        }
    }
}
```

# Step 10: ApprovedPaymentConsumer.java

Package:

```text
com.example.payment.consumer
```

```java
package com.example.payment.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ApprovedPaymentConsumer {

    @KafkaListener(topics = "approved-payments-topic", groupId = "approved-payment-group")
    public void consume(ConsumerRecord<String, String> record) {

        System.out.println("Approved Payment Consumer Received:");
        System.out.println("Topic: " + record.topic());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("--------------------------------------");
    }
}
```

# Step 11: DltConsumer.java

Package:

```text
com.example.payment.consumer
```

```java
package com.example.payment.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class DltConsumer {

    @KafkaListener(topics = "payments-dlt", groupId = "payment-dlt-group")
    public void consume(ConsumerRecord<String, String> record) {

        System.out.println("DEAD LETTER TOPIC Consumer Received Failed Payment:");
        System.out.println("Topic: " + record.topic());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("--------------------------------------");
    }
}
```

# Step 12: PaymentController.java

Package:

```text
com.example.payment.controller
```

```java
package com.example.payment.controller;

import com.example.payment.model.PaymentEvent;
import com.example.payment.producer.PaymentProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Controller
public class PaymentController {

    private final PaymentProducer paymentProducer;

    public PaymentController(PaymentProducer paymentProducer) {
        this.paymentProducer = paymentProducer;
    }

    @GetMapping("/")
    public String showPaymentPage() {
        return "payment";
    }

    @PostMapping("/payments")
    public String makePayment(
            @RequestParam String customerName,
            @RequestParam double amount,
            @RequestParam String cardNumber,
            Model model) {

        String paymentId = "PAY-" + UUID.randomUUID().toString().substring(0, 5);

        PaymentEvent paymentEvent = new PaymentEvent(
                paymentId,
                customerName,
                amount,
                cardNumber,
                "PENDING",
                "PAYMENT_INITIATED"
        );

        paymentProducer.sendPayment(paymentEvent);

        model.addAttribute("message", "Payment submitted to Kafka.");
        model.addAttribute("paymentId", paymentId);
        model.addAttribute("amount", amount);

        return "payment";
    }
}
```

# Step 13: payment.html

File:

```text
src/main/resources/templates/payment.html
```

```html
<!DOCTYPE html>
<html xmlns:th="http://www.thymeleaf.org">
<head>
    <title>Payment Fraud Detection DLQ</title>

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
            background: #c62828;
            color: white;
            border: none;
            cursor: pointer;
            font-size: 16px;
            border-radius: 5px;
        }

        button:hover {
            background: #8e0000;
        }

        .success {
            margin-top: 20px;
            background: #e8f5e9;
            color: #1b5e20;
            padding: 15px;
            border-radius: 8px;
        }

        .info {
            background: #fff3e0;
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

        .hint {
            background: #ffebee;
            padding: 12px;
            border-radius: 8px;
            color: #b71c1c;
            font-weight: bold;
        }
    </style>
</head>
<body>

<h1>Payment Fraud Detection with Retry and DLQ</h1>

<div class="container">

    <div class="card">

        <h2>Make Payment</h2>

        <form method="post" action="/payments">

            <label>Customer Name</label>
            <input type="text" name="customerName" placeholder="Customer Name" required>

            <label>Amount</label>
            <input type="number" step="0.01" name="amount" placeholder="Payment Amount" required>

            <label>Card Number</label>
            <select name="cardNumber" required>
                <option value="">Select Card</option>
                <option value="4111-XXXX-XXXX-1111">4111-XXXX-XXXX-1111</option>
                <option value="5500-XXXX-XXXX-0004">5500-XXXX-XXXX-0004</option>
                <option value="3400-XXXX-XXXX-009">3400-XXXX-XXXX-009</option>
            </select>

            <button type="submit">Submit Payment</button>

        </form>

        <div class="success" th:if="${message}">
            <h3>Payment Submitted</h3>
            <p th:text="${message}"></p>
            <p>Payment ID: <b th:text="${paymentId}"></b></p>
            <p>Amount: <b th:text="${amount}"></b></p>
        </div>

    </div>

    <div class="card">

        <h2>Kafka Retry + DLQ Concept</h2>

        <div class="info">

            <p><b>Main Topic:</b> <code>payments-topic</code></p>
            <p><b>Success Topic:</b> <code>approved-payments-topic</code></p>
            <p><b>Dead Letter Topic:</b> <code>payments-dlt</code></p>

            <hr>

            <p class="hint">
                Amount ≤ 10000 → Approved
                <br>
                Amount > 10000 → Retry → DLQ
            </p>

            <h3>Success Flow</h3>

            <pre>
Payment UI
   ↓
payments-topic
   ↓
FraudDetectionConsumer
   ↓
approved-payments-topic
            </pre>

            <h3>Failure Flow</h3>

            <pre>
Payment UI
   ↓
payments-topic
   ↓
FraudDetectionConsumer
   ↓
Retry 1
Retry 2
Retry 3
   ↓
payments-dlt
            </pre>

            <h3>Key Learning</h3>

            <ul>
                <li>Consumer errors should not destroy events.</li>
                <li>Kafka can retry failed processing.</li>
                <li>After retries fail, message goes to DLQ.</li>
                <li>DLQ is used for investigation and manual recovery.</li>
            </ul>

        </div>

    </div>

</div>

</body>
</html>
```

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
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:9092
```

# Step 15: Run App

Run:

```text
PaymentFraudDlqUiApplication
```

Open:

```text
http://localhost:8085
```

# Step 16: Success Test

Enter:

```text
Customer: Abdullah
Amount: 5000
Card: any
```

Expected:

```text
Payment Event Published
Fraud Detection Consumer Received
Payment Approved and Published
Approved Payment Consumer Received
```

# Step 17: Failure Test

Enter:

```text
Customer: Ahmed
Amount: 50000
Card: any
```

Expected:

```text
Fraud Detection Consumer Received
Fraud check failed
Retry after 2 seconds
Retry after 2 seconds
Retry after 2 seconds
DEAD LETTER TOPIC Consumer Received Failed Payment
```

# Classroom Summary

```text
Normal payment:
payments-topic → Fraud Consumer → approved-payments-topic

Suspicious payment:
payments-topic → Fraud Consumer → retries → payments-dlt
```
