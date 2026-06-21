Great. First UI will be:

# UI 1: Hospital Registration Portal

## Goal

Build a simple Java Spring Boot UI where:

```text
User enters patient details
        ↓
Spring Boot publishes event to Kafka
        ↓
Multiple consumers receive the event
```

---

# Final Flow

```text
Browser UI
   ↓
Spring Boot Controller
   ↓
Kafka Producer
   ↓
patient-events topic
   ↓
Billing Consumer
Doctor Consumer
Insurance Consumer
```

---

# Step 1: Create New Spring Boot Project

Inside IntelliJ:

```text
File → New → Project
```

Select:

```text
Spring Boot
```

Project name:

```text
hospital-registration-ui
```

Java:

```text
17
```

Build tool:

```text
Maven
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

Create this structure:

```text
src/main/java/com/example/hospital/

├── HospitalRegistrationUiApplication.java
├── controller/
│   └── PatientController.java
├── model/
│   └── PatientEvent.java
├── producer/
│   └── PatientProducer.java
├── consumer/
│   ├── BillingConsumer.java
│   ├── DoctorConsumer.java
│   └── InsuranceConsumer.java
└── config/
    └── KafkaTopicConfig.java

src/main/resources/
├── templates/
│   └── register.html
└── application.properties
```

---


#  HospitalRegistrationUiApplication

```java
package com.example.hospital;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class HospitalRegistrationUiApplication {

    public static void main(String[] args) {
        SpringApplication.run(HospitalRegistrationUiApplication.class, args);
    }

}
```

---

# Step 3: application.properties

```properties
spring.application.name=hospital-registration-ui

server.port=8081

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.consumer.group-id=hospital-ui-group
spring.kafka.consumer.auto-offset-reset=earliest

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

---

# Step 4: Create Topic Config

```java
package com.example.hospital.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic patientEventsTopic() {
        return new NewTopic("patient-events", 3, (short) 1);
    }
}
```

---

# Step 5: Create PatientEvent Model

```java
package com.example.hospital.model;

public class PatientEvent {

    private String patientId;
    private String patientName;
    private String department;
    private String eventType;

    public PatientEvent() {
    }

    public PatientEvent(String patientId, String patientName, String department, String eventType) {
        this.patientId = patientId;
        this.patientName = patientName;
        this.department = department;
        this.eventType = eventType;
    }

    public String getPatientId() {
        return patientId;
    }

    public String getPatientName() {
        return patientName;
    }

    public String getDepartment() {
        return department;
    }

    public String getEventType() {
        return eventType;
    }
}
```

---

# Step 6: Create Producer

```java
package com.example.hospital.producer;

import com.example.hospital.model.PatientEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PatientProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PatientProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPatientEvent(PatientEvent patientEvent) {
        try {
            String eventJson = objectMapper.writeValueAsString(patientEvent);
            kafkaTemplate.send("patient-events", patientEvent.getPatientId(), eventJson);
            System.out.println("Published Event: " + eventJson);
        } catch (Exception e) {
            throw new RuntimeException("Error publishing patient event", e);
        }
    }
}
```

---

# Step 7: Create Controller

```java
package com.example.hospital.controller;

import com.example.hospital.model.PatientEvent;
import com.example.hospital.producer.PatientProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Controller
public class PatientController {

    private final PatientProducer patientProducer;

    public PatientController(PatientProducer patientProducer) {
        this.patientProducer = patientProducer;
    }

    @GetMapping("/")
    public String showForm() {
        return "register";
    }

    @PostMapping("/register")
    public String registerPatient(
            @RequestParam String patientName,
            @RequestParam String department,
            Model model) {

        String patientId = "PAT-" + UUID.randomUUID().toString().substring(0, 5);

        PatientEvent event = new PatientEvent(
                patientId,
                patientName,
                department,
                "PATIENT_REGISTERED"
        );

        patientProducer.sendPatientEvent(event);

        model.addAttribute("message", "Patient registered and event published successfully.");
        model.addAttribute("patientId", patientId);

        return "register";
    }
}
```

---

# Step 8: Create UI Page

File:

```text
src/main/resources/templates/register.html
```

```html
<!DOCTYPE html>
<html>
<head>
    <title>Hospital Registration Portal</title>
    <style>
        body {
            font-family: Arial;
            margin: 40px;
            background-color: #f5f7fa;
        }
        .card {
            width: 420px;
            background: white;
            padding: 25px;
            border-radius: 10px;
        }
        input, select, button {
            width: 100%;
            padding: 10px;
            margin-top: 12px;
        }
        button {
            background: #1976d2;
            color: white;
            border: none;
            cursor: pointer;
        }
        .success {
            margin-top: 20px;
            color: green;
            font-weight: bold;
        }
    </style>
</head>
<body>

<div class="card">
    <h2>Hospital Registration Portal</h2>

    <form method="post" action="/register">
        <input type="text" name="patientName" placeholder="Patient Name" required>

        <select name="department" required>
            <option value="">Select Department</option>
            <option value="Cardiology">Cardiology</option>
            <option value="Neurology">Neurology</option>
            <option value="Orthopedics">Orthopedics</option>
            <option value="General Medicine">General Medicine</option>
        </select>

        <button type="submit">Register Patient</button>
    </form>

    <div class="success" th:if="${message}">
        <p th:text="${message}"></p>
        <p>Patient ID: <span th:text="${patientId}"></span></p>
    </div>
</div>

</body>
</html>
```

---

# Step 9: Create Consumers

## BillingConsumer.java

```java
package com.example.hospital.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class BillingConsumer {

    @KafkaListener(topics = "patient-events", groupId = "billing-service")
    public void consume(String event) {
        System.out.println("Billing Service Received: " + event);
    }
}
```

## DoctorConsumer.java

```java
package com.example.hospital.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class DoctorConsumer {

    @KafkaListener(topics = "patient-events", groupId = "doctor-service")
    public void consume(String event) {
        System.out.println("Doctor Service Received: " + event);
    }
}
```

## InsuranceConsumer.java

```java
package com.example.hospital.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InsuranceConsumer {

    @KafkaListener(topics = "patient-events", groupId = "insurance-service")
    public void consume(String event) {
        System.out.println("Insurance Service Received: " + event);
    }
}
```

---

# Step 10: Start Kafka

Before running Spring Boot:

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

# Step 11: Run Application

In IntelliJ:

```text
Run HospitalRegistrationUiApplication
```

Open browser:

```text
http://localhost:8081
```

Enter:

```text
Patient Name: Ahmed Khan
Department: Cardiology
```

Click:

```text
Register Patient
```

---

# Expected Console Output

```text
Published Event: {"patientId":"PAT-12345","patientName":"Ahmed Khan","department":"Cardiology","eventType":"PATIENT_REGISTERED"}

Billing Service Received: {...}
Doctor Service Received: {...}
Insurance Service Received: {...}
```

---

# Teaching Point

This one UI explains:

```text
UI submits data
Spring Boot acts as Producer
Kafka topic stores event
Multiple consumer groups receive the same event
Producer does not call billing/doctor/insurance directly
```
