# Kafka & Event-Driven Architecture Lab Setup — Windows

## Objective

By the end of this setup, your Windows machine will have:

* Java 17
* Maven
* Node.js
* Git
* IntelliJ IDEA
* Docker Desktop
* Apache Kafka
* Zookeeper
* Kafka UI
* PostgreSQL
* Kafka Connect
* Debezium

This setup is for Kafka / AMQ Streams / Event-Driven Architecture training labs.

---

# 1. System Requirements

Recommended minimum:

```text
RAM: 16 GB minimum, 32 GB preferred
CPU: 4 cores minimum
Disk: 30 GB free space
OS: Windows 10/11 64-bit
Internet: Required for downloading tools and Docker images
```

---

# 2. Install Java 17

Download and install **Eclipse Temurin JDK 17**.

After installation, open **Command Prompt** and run:

```cmd
java -version
javac -version
```

Expected output:

```text
openjdk version "17..."
javac 17...
```

---

# 3. Set JAVA_HOME

Open:

```text
Start Menu → Environment Variables → Edit the system environment variables
```

Click:

```text
Environment Variables
```

Under **System variables**, click **New**:

```text
Variable name: JAVA_HOME
Variable value: C:\Program Files\Eclipse Adoptium\jdk-17...
```

The exact folder may look like:

```text
C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot
```

Then edit the **Path** variable and add:

```text
%JAVA_HOME%\bin
```

Close and reopen Command Prompt.

Verify:

```cmd
echo %JAVA_HOME%
java -version
javac -version
```

---

# 4. Install Maven

Download Apache Maven binary zip.

Extract it to:

```text
C:\Tools\apache-maven-3.9.9
```

Set Maven environment variable.

Open Environment Variables.

Create new **System variable**:

```text
Variable name: MAVEN_HOME
Variable value: C:\Tools\apache-maven-3.9.9
```

Edit **Path** and add:

```text
%MAVEN_HOME%\bin
```

Close and reopen Command Prompt.

Verify:

```cmd
mvn -version
```

Expected output:

```text
Apache Maven 3.9.9
Java version: 17...
```

---

# 5. Install Node.js and npm

Download and install **Node.js LTS**.

After installation, open Command Prompt:

```cmd
node -v
npm -v
```

Expected output:

```text
v20.x or higher
npm 10.x or higher
```

---

# 6. Install Git

Download and install **Git for Windows**.

Use default options during installation.

Verify:

```cmd
git --version
```

---

# 7. Install IntelliJ IDEA

Install **IntelliJ IDEA Community** or **IntelliJ IDEA Ultimate**.

Community is enough for:

```text
Java
Maven
Kafka Producer
Kafka Consumer
Kafka Streams basics
```

Ultimate is better if you want:

```text
Spring Boot
Node.js support
Database tools
REST client features
```

During installation, enable:

```text
Create Desktop Shortcut
Add "bin" folder to PATH
Add "Open Folder as Project"
Associate .java, .xml, .json, .js files
```

Open IntelliJ once and confirm it launches successfully.

---

# 8. Install Docker Desktop

Install Docker Desktop for Windows.

During installation, enable:

```text
Use WSL 2 based engine
```

After installation, restart Windows if asked.

Open Docker Desktop and wait until it shows:

```text
Docker Desktop is running
```

Verify from Command Prompt:

```cmd
docker --version
docker compose version
```

Run Docker test:

```cmd
docker run hello-world
```

Expected output:

```text
Hello from Docker!
```

---

# 9. Create Kafka Training Folder

Open Command Prompt and run:

```cmd
mkdir C:\kafka-amq-training
cd C:\kafka-amq-training

mkdir apps
mkdir labs
mkdir data
mkdir scripts
```

Verify:

```cmd
cd
dir
```

Expected folder:

```text
C:\kafka-amq-training
```

---

# 10. Create Docker Compose File

Inside:

```text
C:\kafka-amq-training
```

create a file named:

```text
docker-compose.yml
```

You can create it using Notepad:

```cmd
notepad docker-compose.yml
```

Paste the following:

```yaml
version: "3.8"

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.1
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: training-cluster
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092

  postgres:
    image: postgres:15
    container_name: postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: training
      POSTGRES_PASSWORD: training
      POSTGRES_DB: trainingdb
    command: ["postgres", "-c", "wal_level=logical"]

  kafka-connect:
    image: debezium/connect:2.6
    container_name: kafka-connect
    depends_on:
      - kafka
      - postgres
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: connect-cluster
      CONFIG_STORAGE_TOPIC: connect-configs
      OFFSET_STORAGE_TOPIC: connect-offsets
      STATUS_STORAGE_TOPIC: connect-status
```

Save and close Notepad.

---

# 11. Start Kafka Lab Stack

Open Command Prompt:

```cmd
cd C:\kafka-amq-training
docker compose up -d
```

Docker will download and start:

```text
Zookeeper
Kafka
Kafka UI
PostgreSQL
Kafka Connect / Debezium
```

Check running containers:

```cmd
docker ps
```

You should see:

```text
zookeeper
kafka
kafka-ui
postgres
kafka-connect
```

---

# 12. Verify Kafka UI

Open browser and go to:

```text
http://localhost:8080
```

You should see:

```text
UI for Apache Kafka
```

Cluster name should be:

```text
training-cluster
```

---

# 13. Verify Kafka Connect

In Command Prompt, run:

```cmd
curl http://localhost:8083/connectors
```

Expected output:

```json
[]
```

If `curl` is not available in your Command Prompt, use PowerShell:

```powershell
Invoke-WebRequest http://localhost:8083/connectors
```

Expected response body:

```json
[]
```

---

# 14. Create Kafka Topic

Run:

```cmd
docker exec -it kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Expected output:

```text
Created topic orders.
```

List topics:

```cmd
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

Expected output includes:

```text
orders
```

You may also see internal topics:

```text
__consumer_offsets
connect-configs
connect-offsets
connect-status
```

That is normal.

---

# 15. Produce Messages from Command Line

Run:

```cmd
docker exec -it kafka kafka-console-producer --topic orders --bootstrap-server localhost:9092
```

Type these messages:

```text
order-1
order-2
order-3
```

Press:

```text
Ctrl + C
```

---

# 16. Consume Messages from Command Line

Run:

```cmd
docker exec -it kafka kafka-console-consumer --topic orders --from-beginning --bootstrap-server localhost:9092
```

Expected output:

```text
order-1
order-2
order-3
```

Stop consumer:

```text
Ctrl + C
```

---

# 17. Verify Messages in Kafka UI

Open browser:

```text
http://localhost:8080
```

Go to:

```text
Topics → orders → Messages
```

Select:

```text
Key Serde: String
Value Serde: String
```

Click:

```text
Submit
```

You should see messages in the `orders` topic.

---

# 18. Create Start Script for Windows

Inside:

```text
C:\kafka-amq-training
```

create a file:

```text
start-lab.bat
```

Run:

```cmd
notepad start-lab.bat
```

Paste:

```bat
@echo off

cd /d C:\kafka-amq-training

docker compose up -d

echo.
echo Kafka lab started.
echo Kafka UI: http://localhost:8080
echo Kafka Connect: http://localhost:8083/connectors
echo.

docker ps

pause
```

Save and close.

Run it:

```cmd
start-lab.bat
```

---

# 19. Create Stop Script for Windows

Create:

```text
stop-lab.bat
```

Run:

```cmd
notepad stop-lab.bat
```

Paste:

```bat
@echo off

cd /d C:\kafka-amq-training

docker compose down

echo.
echo Kafka lab stopped.
echo.

pause
```

Save and close.

Run:

```cmd
stop-lab.bat
```

Start again:

```cmd
start-lab.bat
```

---

# 20. Create Useful Kafka Topics

Run:

```cmd
docker exec -it kafka kafka-topics --create --if-not-exists --topic orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics --create --if-not-exists --topic payments --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics --create --if-not-exists --topic shipments --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics --create --if-not-exists --topic customer-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics --create --if-not-exists --topic high-value-orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics --create --if-not-exists --topic dead-letter-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

List topics:

```cmd
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

---

# 21. Create Java Kafka Project in IntelliJ

Open IntelliJ IDEA.

Choose:

```text
New Project
```

Select:

```text
Java
```

Use:

```text
Build system: Maven
JDK: Java 17
```

Project details:

```text
Name: java-kafka-basics
Location: C:\kafka-amq-training\apps
GroupId: com.training.kafka
ArtifactId: java-kafka-basics
```

The project path should become:

```text
C:\kafka-amq-training\apps\java-kafka-basics
```

Uncheck:

```text
Add sample code
```

Click:

```text
Create
```

---

# 22. Configure pom.xml

Open `pom.xml` and replace its content with:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">

    <modelVersion>4.0.0</modelVersion>

    <groupId>com.training.kafka</groupId>
    <artifactId>java-kafka-basics</artifactId>
    <version>1.0.0</version>

    <properties>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <kafka.version>3.7.0</kafka.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>${kafka.version}</version>
        </dependency>

        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>1.7.36</version>
        </dependency>
    </dependencies>

</project>
```

Right-click `pom.xml`:

```text
Maven → Sync Project
```

or click:

```text
Load Maven Changes / Sync Project
```

---

# 23. Create Java Package

In IntelliJ:

```text
src → main → java
```

Right-click `java`:

```text
New → Package
```

Package name:

```text
com.training.kafka
```

---

# 24. Create OrderProducer

Right-click package:

```text
com.training.kafka → New → Java Class
```

Class name:

```text
OrderProducer
```

Paste:

```java
package com.training.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.util.Properties;

public class OrderProducer {

    public static void main(String[] args) {

        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 1; i <= 10; i++) {

            String key = "order-" + i;

            String value = """
                    {"orderId":%d,"customer":"Customer-%d","amount":%d,"createdAt":"%s"}
                    """.formatted(i, i, i * 100, LocalDateTime.now());

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("orders", key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Sent message:");
                    System.out.println("Topic: " + metadata.topic());
                    System.out.println("Partition: " + metadata.partition());
                    System.out.println("Offset: " + metadata.offset());
                    System.out.println("-----------------------------");
                } else {
                    exception.printStackTrace();
                }
            });
        }

        producer.flush();
        producer.close();

        System.out.println("All orders sent successfully.");
    }
}
```

Run:

```text
Right-click inside file → Run 'OrderProducer.main()'
```

Expected output:

```text
All orders sent successfully.
```

---

# 25. Create OrderConsumer

Right-click package:

```text
com.training.kafka → New → Java Class
```

Class name:

```text
OrderConsumer
```

Paste:

```java
package com.training.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class OrderConsumer {

    public static void main(String[] args) {

        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:29092");
        props.put("group.id", "order-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("orders"));

        System.out.println("Listening for orders... Stop from IntelliJ when done.");

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received message:");
                System.out.println("Key: " + record.key());
                System.out.println("Value: " + record.value());
                System.out.println("Partition: " + record.partition());
                System.out.println("Offset: " + record.offset());
                System.out.println("-----------------------------");
            }
        }
    }
}
```

Run:

```text
Right-click inside file → Run 'OrderConsumer.main()'
```

Keep it running.

Then run `OrderProducer`.

Expected:

```text
Consumer receives order messages
Producer sends order messages
Kafka UI shows messages
```

---

# 26. Running Producer and Consumer Together

In IntelliJ:

1. Run `OrderConsumer`
2. Keep it running
3. Open `OrderProducer.java`
4. Right-click → Run `OrderProducer.main()`

If IntelliJ reuses the same window:

```text
Top-right run dropdown → Edit Configurations
```

Enable:

```text
Allow multiple instances
```

or:

```text
Allow parallel run
```

Alternative:

Run consumer in IntelliJ and producer from terminal:

```cmd
cd C:\kafka-amq-training\apps\java-kafka-basics
mvn compile exec:java -Dexec.mainClass="com.training.kafka.OrderProducer"
```

---

# 27. Final Verification Checklist

Run:

```cmd
java -version
javac -version
mvn -version
node -v
npm -v
git --version
docker --version
docker compose version
docker ps
curl http://localhost:8083/connectors
```

Expected:

```text
Java 17 works
Maven works
Node.js works
npm works
Git works
Docker works
Docker Compose works
Kafka containers are running
Kafka Connect returns []
Kafka UI opens at http://localhost:8080
Java producer sends messages
Java consumer receives messages
Kafka UI shows messages
```

---

# 28. Common URLs

Kafka UI:

```text
http://localhost:8080
```

Kafka Connect:

```text
http://localhost:8083/connectors
```

PostgreSQL:

```text
host: localhost
port: 5432
database: trainingdb
username: training
password: training
```

Kafka bootstrap server for local Java/Node.js applications:

```text
localhost:29092
```

Kafka bootstrap server for containers:

```text
kafka:9092
```

---

# 29. Daily Lab Start

Before starting labs:

```cmd
cd C:\kafka-amq-training
start-lab.bat
```

Open:

```text
http://localhost:8080
```

---

# 30. Daily Lab Stop

After labs:

```cmd
cd C:\kafka-amq-training
stop-lab.bat
```

---

# 31. Troubleshooting

## Docker Desktop not running

Open Docker Desktop manually.

Then check:

```cmd
docker ps
```

## Kafka UI not opening

Check containers:

```cmd
docker ps
```

Check Kafka UI logs:

```cmd
docker logs kafka-ui
```

Restart Kafka UI:

```cmd
docker compose restart kafka-ui
```

## Kafka not starting

Check Kafka logs:

```cmd
docker logs kafka
```

Restart stack:

```cmd
docker compose down
docker compose up -d
```

## Kafka Connect not responding

Check logs:

```cmd
docker logs kafka-connect
```

Restart:

```cmd
docker compose restart kafka-connect
```

## Port already in use

In Command Prompt:

```cmd
netstat -ano | findstr :8080
netstat -ano | findstr :29092
netstat -ano | findstr :5432
```

Stop containers:

```cmd
cd C:\kafka-amq-training
docker compose down
```

Start again:

```cmd
docker compose up -d
```

## Clean everything and restart fresh

Warning: this removes containers and volumes.

```cmd
cd C:\kafka-amq-training
docker compose down -v
docker compose up -d
```

---

# 32. Setup Complete

Your Windows machine is ready for the Kafka and Event-Driven Architecture training labs.

Installed and configured:

```text
Java 17
Maven
Node.js
npm
Git
IntelliJ IDEA
Docker Desktop
Kafka
Zookeeper
Kafka UI
PostgreSQL
Kafka Connect
Debezium
Java Producer
Java Consumer
```
