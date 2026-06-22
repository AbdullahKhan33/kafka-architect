# Kafka & Event-Driven Architecture Lab Setup — macOS

## Objective

By the end of this setup, your Mac will have:

* Java 17
* Maven
* Node.js
* Git
* IntelliJ IDEA
* Docker Desktop
* Kafka
* Zookeeper
* Kafka UI
* PostgreSQL
* Kafka Connect
* Debezium

This setup is for the Kafka / AMQ Streams / Event-Driven Architecture training labs.

---

# 1. Check Mac Architecture

Open **Terminal** and run:

```bash
uname -m
```

If output is:

```text
arm64
```

You are using Apple Silicon Mac.

If output is:

```text
x86_64
```

You are using Intel Mac.

Both are fine.

---

# 2. Install Homebrew

Run:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

After installation, run:

```bash
brew --version
```

If Homebrew is installed correctly, you should see a version number.

---

# 3. Install Java 17

Run:

```bash
brew install --cask temurin@17
```

Verify Java:

```bash
java -version
javac -version
```

Expected output should show Java 17:

```text
openjdk version "17..."
javac 17...
```

---

# 4. Set JAVA_HOME

Run:

```bash
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 17)' >> ~/.zshrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.zshrc
source ~/.zshrc
```

Verify:

```bash
echo $JAVA_HOME
java -version
```

---

# 5. Install Maven

Run:

```bash
brew install maven
```

Verify:

```bash
mvn -version
```

Expected output:

```text
Apache Maven ...
Java version: 17...
```

---

# 6. Install Node.js and npm

Run:

```bash
brew install node
```

Verify:

```bash
node -v
npm -v
```

Expected output:

```text
v20.x or higher
npm 10.x or higher
```

---

# 7. Install Git

Run:

```bash
brew install git
```

Verify:

```bash
git --version
```

---

# 8. Install IntelliJ IDEA

For Java-based Kafka development, install IntelliJ IDEA.

Community edition is enough for Java and Maven labs.

Run:

```bash
brew install --cask intellij-idea-ce
```

If you have Ultimate license/trial, install Ultimate instead:

```bash
brew install --cask intellij-idea
```

Open IntelliJ:

```bash
open -a "IntelliJ IDEA CE"
```

or for Ultimate:

```bash
open -a "IntelliJ IDEA"
```

---

# 9. Install Docker Desktop

Run:

```bash
brew install --cask docker
```

Open Docker Desktop:

```bash
open -a Docker
```

Wait until Docker Desktop fully starts.

You should see Docker running in the top menu bar.

Verify Docker:

```bash
docker --version
docker compose version
```

Run Docker test:

```bash
docker run hello-world
```

Expected output:

```text
Hello from Docker!
```

---

# 10. Create Kafka Training Folder

Run:

```bash
mkdir -p ~/kafka-amq-training
cd ~/kafka-amq-training

mkdir -p apps
mkdir -p labs
mkdir -p data
mkdir -p scripts
```

Verify:

```bash
pwd
ls
```

Expected folder:

```text
/Users/<your-user>/kafka-amq-training
```

---

# 11. Create Docker Compose File

Inside the training folder:

```bash
cd ~/kafka-amq-training
nano docker-compose.yml
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
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

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

Save the file:

```text
Ctrl + O
Enter
Ctrl + X
```

---

# 12. Start Kafka Lab Stack

Run:

```bash
cd ~/kafka-amq-training
docker compose up -d
```

Wait for all images to download and containers to start.

Check running containers:

```bash
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

# 13. Verify Kafka UI

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

# 14. Verify Kafka Connect

Run:

```bash
curl http://localhost:8083/connectors
```

Expected output:

```json
[]
```

This means Kafka Connect is running.

---

# 15. Create Kafka Topic

Run:

```bash
docker exec -it kafka kafka-topics --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

Expected output:

```text
Created topic orders.
```

List topics:

```bash
docker exec -it kafka kafka-topics --list \
  --bootstrap-server localhost:9092
```

You should see:

```text
orders
```

You may also see internal topics like:

```text
__consumer_offsets
connect-configs
connect-offsets
connect-status
```

That is normal.

---

# 16. Produce Messages from Command Line

Run:

```bash
docker exec -it kafka kafka-console-producer \
  --topic orders \
  --bootstrap-server localhost:9092
```

Type these messages one by one:

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

# 17. Consume Messages from Command Line

Run:

```bash
docker exec -it kafka kafka-console-consumer \
  --topic orders \
  --from-beginning \
  --bootstrap-server localhost:9092
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

# 18. Verify Messages in Kafka UI

Open:

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

You should see the messages.

---

# 19. Create Start Script

Run:

```bash
cd ~/kafka-amq-training
nano start-lab.sh
```

Paste:

```bash
#!/bin/bash

cd ~/kafka-amq-training

docker compose up -d

echo ""
echo "Kafka lab started."
echo "Kafka UI: http://localhost:8080"
echo "Kafka Connect: http://localhost:8083/connectors"
echo ""

docker ps
```

Save:

```text
Ctrl + O
Enter
Ctrl + X
```

Make executable:

```bash
chmod +x start-lab.sh
```

Run:

```bash
./start-lab.sh
```

---

# 20. Create Stop Script

Run:

```bash
cd ~/kafka-amq-training
nano stop-lab.sh
```

Paste:

```bash
#!/bin/bash

cd ~/kafka-amq-training

docker compose down

echo ""
echo "Kafka lab stopped."
echo ""
```

Save:

```text
Ctrl + O
Enter
Ctrl + X
```

Make executable:

```bash
chmod +x stop-lab.sh
```

Run:

```bash
./stop-lab.sh
```

Start again:

```bash
./start-lab.sh
```

---

# 21. Create Useful Topics

Run:

```bash
docker exec -it kafka kafka-topics --create --if-not-exists \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

docker exec -it kafka kafka-topics --create --if-not-exists \
  --topic payments \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

docker exec -it kafka kafka-topics --create --if-not-exists \
  --topic shipments \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

docker exec -it kafka kafka-topics --create --if-not-exists \
  --topic customer-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

docker exec -it kafka kafka-topics --create --if-not-exists \
  --topic high-value-orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

docker exec -it kafka kafka-topics --create --if-not-exists \
  --topic dead-letter-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

List topics:

```bash
docker exec -it kafka kafka-topics --list \
  --bootstrap-server localhost:9092
```

---

# 22. Final Verification Checklist

Run:

```bash
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
```

---

# 23. Common URLs

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

Kafka bootstrap server for local applications:

```text
localhost:29092
```

Kafka bootstrap server for containers:

```text
kafka:9092
```

---

# 24. Daily Lab Start Command

Every day before starting labs, run:

```bash
cd ~/kafka-amq-training
./start-lab.sh
```

Open Kafka UI:

```text
http://localhost:8080
```

---

# 25. Daily Lab Stop Command

After completing labs, run:

```bash
cd ~/kafka-amq-training
./stop-lab.sh
```

---

# 26. Troubleshooting

## Docker not running

Start Docker Desktop manually:

```bash
open -a Docker
```

Then check:

```bash
docker ps
```

## Port already in use

Check which process is using port 8080:

```bash
lsof -i :8080
```

Check Kafka port:

```bash
lsof -i :29092
```

Stop Docker containers:

```bash
cd ~/kafka-amq-training
docker compose down
```

Start again:

```bash
docker compose up -d
```

## Kafka UI not opening

Check containers:

```bash
docker ps
```

Check Kafka UI logs:

```bash
docker logs kafka-ui
```

Restart:

```bash
docker compose restart kafka-ui
```

## Kafka not starting

Check Kafka logs:

```bash
docker logs kafka
```

Restart the full stack:

```bash
docker compose down
docker compose up -d
```

## Kafka Connect not responding

Check logs:

```bash
docker logs kafka-connect
```

Restart:

```bash
docker compose restart kafka-connect
```

## Clean everything and restart fresh

Warning: this removes containers and volumes.

```bash
cd ~/kafka-amq-training
docker compose down -v
docker compose up -d
```

---

# 27. Setup Complete

Your Mac is now ready for the Kafka and Event-Driven Architecture training labs.

You have:

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
```
