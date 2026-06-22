

> "Can Kafka itself process data without writing consumers and producers everywhere?"

And the answer is:

> **Kafka Streams**

---

# UI6 : Live Portfolio Dashboard (Kafka Streams)

This UI is fantastic because students immediately see the result.

---

# Scenario

User submits trades:

```text
AAPL 100
MSFT 50
AAPL 25
GOOG 10
MSFT 40
```

Producer sends to:

```text
trades-topic
```

---

# Kafka Streams Engine

Consumes:

```text
trades-topic
```

Performs aggregation:

```text
AAPL = 125

MSFT = 90

GOOG = 10
```

Produces results to:

```text
portfolio-summary-topic
```

---

# Dashboard Consumer

Reads:

```text
portfolio-summary-topic
```

and displays

```text
Portfolio Dashboard

AAPL : 125 shares

MSFT : 90 shares

GOOG : 10 shares
```

Live.

---

# Architecture

```text
UI
 ↓

Trade Producer

 ↓

trades-topic

 ↓

Kafka Streams

groupBy()

↓

count()/aggregate()

↓

portfolio-summary-topic

↓

Dashboard Consumer

↓

UI
```

---

# Concepts Covered

### KStream

```java
KStream<String,String>
```

---

### groupByKey()

```java
stream.groupByKey()
```

---

### aggregate()

```java
.aggregate()
```

---

### Materialized State Store

Kafka stores:

```text
AAPL → 125

MSFT → 90
```

inside RocksDB.

---

### KTable

Represents latest values.

```text
AAPL = 125

MSFT = 90
```

---

### Continuous computation

Unlike batch:

```text
Read once
Compute once
Stop
```

Streams do:

```text
Read forever

Compute forever

Update forever
```

---

# UI Layout

### Left Side

Trade Entry

```text
Stock

[AAPL ▼]

Quantity

[100]

Submit
```

---

### Right Side

Live Portfolio

```text
AAPL 125

MSFT 90

GOOG 10
```

---

# Example

Input

```text
AAPL 100
```

Dashboard

```text
AAPL 100
```

Input

```text
MSFT 40
```

Dashboard

```text
AAPL 100

MSFT 40
```

Input

```text
AAPL 20
```

Dashboard

```text
AAPL 120

MSFT 40
```

> Kafka Streams is continuously updating state.

---

# Concepts Covered

### KStream

### KTable

### groupByKey

### aggregate

### State Store

### Continuous Processing

### Topology

### Streams Application

### Output Topic

---
