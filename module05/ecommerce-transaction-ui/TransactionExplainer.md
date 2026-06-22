Yes — the **main transaction logic is inside**:

```text
src/main/java/com/example/ecommerce/service/TransactionalOrderService.java
```

This is the file to show after the demo.

# UI 4: What happened?

When the user clicks **Place Order Transactionally**, the controller calls:

```java
transactionalOrderService.placeOrder(...)
```

That method publishes two events:

```text
1. ORDER_PLACED          → ecommerce-orders-topic
2. INVENTORY_RESERVED    → ecommerce-inventory-topic
```

Both are inside **one Kafka transaction**.

---

# The main code

```java
@Transactional
public String placeOrder(
        String customerName,
        String productName,
        int quantity,
        double price,
        boolean simulateFailure) {

    String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 5);

    OrderEvent orderEvent = new OrderEvent(
            orderId,
            customerName,
            productName,
            quantity,
            price,
            "ORDER_PLACED"
    );

    InventoryEvent inventoryEvent = new InventoryEvent(
            "INV-" + UUID.randomUUID().toString().substring(0, 5),
            orderId,
            productName,
            quantity,
            "INVENTORY_RESERVED"
    );

    String orderJson = objectMapper.writeValueAsString(orderEvent);
    String inventoryJson = objectMapper.writeValueAsString(inventoryEvent);

    kafkaTemplate.send(
            "ecommerce-orders-topic",
            orderId,
            orderJson
    );

    if (simulateFailure) {
        throw new RuntimeException("Simulated failure after order event.");
    }

    kafkaTemplate.send(
            "ecommerce-inventory-topic",
            orderId,
            inventoryJson
    );

    return orderId;
}
```

---

# The most important line

```java
@Transactional
```

This tells Spring:

```text
Start a Kafka transaction before this method runs.

If the method completes successfully:
    COMMIT the transaction.

If the method throws an exception:
    ROLLBACK / ABORT the transaction.
```

---

# Success case

Checkbox **not selected**.

```java
kafkaTemplate.send("ecommerce-orders-topic", orderId, orderJson);

kafkaTemplate.send("ecommerce-inventory-topic", orderId, inventoryJson);
```

Both lines complete.

So Spring commits the transaction.

```text
BEGIN TRANSACTION

orders-topic receives ORDER_PLACED

inventory-topic receives INVENTORY_RESERVED

COMMIT
```

Consumers now see both events.

---

# Failure case

Checkbox **selected**.

This line runs:

```java
kafkaTemplate.send(
        "ecommerce-orders-topic",
        orderId,
        orderJson
);
```

Then this block runs:

```java
if (simulateFailure) {
    throw new RuntimeException("Simulated failure after order event.");
}
```

Because an exception is thrown, this line never runs:

```java
kafkaTemplate.send(
        "ecommerce-inventory-topic",
        orderId,
        inventoryJson
);
```

Spring sees the exception and aborts the Kafka transaction.

```text
BEGIN TRANSACTION

orders-topic attempted ORDER_PLACED

exception thrown

ABORT TRANSACTION
```

So the order event is not visible to consumers.

---

# Who hides aborted messages?

This property:

```properties
spring.kafka.consumer.properties.isolation.level=read_committed
```

It tells consumers:

```text
Only read committed transaction messages.
Ignore aborted transaction messages.
```

So in failure case:

```text
OrderConsumer does not receive ORDER_PLACED.
InventoryConsumer does not receive INVENTORY_RESERVED.
```

---

# The final classroom summary

```text
@Transactional is the boundary.

Inside that boundary, we send to two Kafka topics.

If everything succeeds, Kafka commits both events.

If any exception happens, Kafka aborts the entire transaction.

Consumers configured with read_committed only see committed events.

So Kafka protects us from partial publishing across topics.
```
