

# Step 1: Find docker-compose.yml

Open Terminal and run:

```bash
find /Users/abdullahkhan -name "docker-compose.yml"
```

You'll probably get something like:

```text
/Users/abdullahkhan/kafka_training/docker-compose.yml
```

or

```text
/Users/abdullahkhan/Documents/kafka_training/docker-compose.yml
```

---

# Step 2: Go to that folder

Example:

```bash
cd /Users/abdullahkhan/kafka_training
```

Verify:

```bash
ls
```

You should see:

```text
docker-compose.yml
```

---

# Step 3: Open the compose file

```bash
nano docker-compose.yml
```

---

# Step 4: Find the kafka section

You'll see something similar to:

```yaml
kafka:
  image: confluentinc/cp-kafka:7.6.1
  container_name: kafka
  environment:
```

Inside environment add:

```yaml
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

Example:

```yaml
environment:
  KAFKA_BROKER_ID: 1
  KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

---

# Step 5: Save nano

Press:

```text
Ctrl+O
```

Press Enter

Then:

```text
Ctrl+X
```

---

# Step 6: Restart Kafka

From the same folder:

```bash
docker compose down
docker compose up -d
```

Wait about 20 seconds.

---

# Step 7: Verify containers

```bash
docker ps
```

You should see:

```text
zookeeper
kafka
kafka-ui
postgres
```

---

# Step 8: Verify transaction topic

Run:

```bash
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list
```

Look for:

```text
__transaction_state
```

If it is present, your broker is transaction-capable.

---


### ✅ Add them under the Kafka section

Find:

```yaml
kafka:
  image: confluentinc/cp-kafka:7.6.1
  container_name: kafka
  depends_on:
    - zookeeper
  ports:
```

and locate its `environment:` block. Add:

```yaml
environment:
  KAFKA_BROKER_ID: 1
  KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

It should look roughly like:

```yaml
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
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

Then:

### Save

```
Ctrl+O
Enter
Ctrl+X
```

### Restart

```bash
docker compose down
docker compose up -d
```

Then run:

```bash
docker ps
```

and send me a screenshot or paste the entire **kafka section** of your `docker-compose.yml`. I can verify the rest before you start the Spring Boot app again.
