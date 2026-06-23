
# Bucket 1: How the workflow sequencing is happening

The idea is:

```text
One consumer does not do everything.

Each consumer listens to one topic.
After processing, it publishes the next event to the next topic.
That next topic wakes up the next consumer.
```

## Full flow

```text
Place Order
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
   ↓
FinalDeliveryConsumer
```

---

## 1. User clicks Place Order

File:

```text
WorkflowController.java
```

Code:

```java
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
```

What it does:

```text
The UI form submits customer name, product name, amount.
Controller calls workflowEventProducer.placeOrder().
```

---

## 2. Order event is published to the first topic

File:

```text
WorkflowEventProducer.java
```

Code:

```java
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
```

This publishes:

```text
Topic      = order-events-topic
Key        = orderId
Event Type = ORDER_PLACED
Status     = ORDER_PLACED
```

Actual Kafka send happens here:

```java
kafkaTemplate.send(
        topic,
        orderId,
        eventJson
);
```

Important point:

```text
orderId is used as Kafka key.
This helps events for the same order stay consistently routed.
```

---

## 3. PaymentConsumer wakes up

File:

```text
PaymentConsumer.java
```

Code:

```java
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
}
```

What is happening:

```text
PaymentConsumer listens to order-events-topic.

When ORDER_PLACED arrives, it processes payment.

Then it publishes PAYMENT_RECEIVED to payment-events-topic.
```

The next event is fired from:

```java
workflowEventProducer.publishPaymentReceived(event);
```

---

## 4. Payment received event is published

File:

```text
WorkflowEventProducer.java
```

Code:

```java
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
```

This publishes:

```text
Topic      = payment-events-topic
Event Type = PAYMENT_RECEIVED
Status     = PAYMENT_RECEIVED
```

---

## 5. PackingConsumer wakes up

File:

```text
PackingConsumer.java
```

Code:

```java
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
}
```

What is happening:

```text
PackingConsumer listens only to payment-events-topic.

So packing starts only after PAYMENT_RECEIVED exists.
```

This is the sequencing logic.

Then this fires the next event:

```java
workflowEventProducer.publishOrderPacked(event);
```

---

## 6. Packed event is published

File:

```text
WorkflowEventProducer.java
```

Code:

```java
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
```

This publishes:

```text
Topic      = packing-events-topic
Event Type = ORDER_PACKED
Status     = ORDER_PACKED
```

---

## 7. ShippingConsumer wakes up

File:

```text
ShippingConsumer.java
```

Code:

```java
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
}
```

What is happening:

```text
ShippingConsumer listens only to packing-events-topic.

So shipping starts only after ORDER_PACKED exists.
```

---

## 8. Shipped event is published

File:

```text
WorkflowEventProducer.java
```

Code:

```java
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
```

This publishes:

```text
Topic      = shipping-events-topic
Event Type = ORDER_SHIPPED
Status     = ORDER_SHIPPED
```

---

## 9. DeliveryConsumer wakes up

File:

```text
DeliveryConsumer.java
```

Code:

```java
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
}
```

What is happening:

```text
DeliveryConsumer listens only to shipping-events-topic.

So delivery starts only after ORDER_SHIPPED exists.
```

---

## 10. Delivered event is published

File:

```text
WorkflowEventProducer.java
```

Code:

```java
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
```

This publishes:

```text
Topic      = delivery-events-topic
Event Type = DELIVERED
Status     = DELIVERED
```

---

## 11. FinalDeliveryConsumer records final status

File:

```text
FinalDeliveryConsumer.java
```

Code:

```java
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
}
```

What is happening:

```text
FinalDeliveryConsumer listens to delivery-events-topic.

It does not publish another event.

It records the final DELIVERED status in the dashboard trace view.
```

---

## Bucket 1 summary

```text
ORDER_PLACED is published to order-events-topic.

PaymentConsumer consumes it and publishes PAYMENT_RECEIVED.

PackingConsumer consumes PAYMENT_RECEIVED and publishes ORDER_PACKED.

ShippingConsumer consumes ORDER_PACKED and publishes ORDER_SHIPPED.

DeliveryConsumer consumes ORDER_SHIPPED and publishes DELIVERED.

FinalDeliveryConsumer consumes DELIVERED and records final status.
```


```text
Kafka is not automatically sequencing the business process.

Sequencing happens because each consumer listens to the previous stage topic and publishes the next stage event.
```

---

# Bucket 2: How tracing is working

Tracing means:

```text
For one order ID, show all events that happened across all topics and services.
```

Example:

```text
ORD-12345
   ↓
ORDER_PLACED
PAYMENT_RECEIVED
ORDER_PACKED
ORDER_SHIPPED
DELIVERED
```

---

## 1. Every event contains orderId and correlationId

File:

```text
OrderWorkflowEvent.java
```

Code:

```java
private String orderId;
private String correlationId;
private String eventType;
private String status;
private String sourceService;
private String eventTime;
```

In our demo:

```java
String correlationId = orderId;
```

So:

```text
orderId       = ORD-ABCDE
correlationId = ORD-ABCDE
```

Simple explanation:

```text
orderId is the business ID.

correlationId is the technical trace ID.

In this demo, both are same.

In real systems, correlationId may be a separate trace ID.
```

---

## 2. Trace model stores event metadata

File:

```text
OrderTraceEvent.java
```

Code:

```java
private String orderId;
private String correlationId;
private String eventType;
private String status;
private String sourceService;
private String topic;
private int partition;
private long offset;
private String eventTime;
```

This object stores:

```text
Which order?
Which event?
Which service?
Which Kafka topic?
Which partition?
Which offset?
At what time?
```

This is what makes tracing possible.

---

## 3. Trace is recorded whenever a consumer processes an event

Example file:

```text
PaymentConsumer.java
```

Code:

```java
orderTraceService.addTrace(
        event,
        record,
        "PaymentConsumer"
);
```

This same line exists in every consumer:

```text
PaymentConsumer.java
PackingConsumer.java
ShippingConsumer.java
DeliveryConsumer.java
FinalDeliveryConsumer.java
```

So every stage records trace metadata.

---

## 4. The main tracing logic

File:

```text
OrderTraceService.java
```

Code:

```java
private final Map<String, List<OrderTraceEvent>> traceStore =
        new ConcurrentHashMap<>();
```

This is the in-memory trace store.

Meaning:

```text
Key   = orderId
Value = list of events for that order
```

Example:

```text
ORD-ABCDE
    → ORDER_PLACED
    → PAYMENT_RECEIVED
    → ORDER_PACKED
    → ORDER_SHIPPED
    → DELIVERED
```

Important clarification:

```text
This HashMap is not Kafka.

It is our application-level trace view.

In production, this should be a database, Elasticsearch, OpenSearch, or Azure AI Search.
```

---

## 5. addTrace() method

File:

```text
OrderTraceService.java
```

Code:

```java
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
}
```

What this does:

```text
1. It takes the event consumed from Kafka.
2. It takes Kafka metadata: topic, partition, offset.
3. It creates an OrderTraceEvent.
4. It stores that trace event under the orderId.
```

So if order ID is:

```text
ORD-ABCDE
```

then the trace gets stored under:

```java
traceStore.get("ORD-ABCDE")
```

---

## 6. Latest order status is also updated

File:

```text
OrderTraceService.java
```

Code:

```java
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
```

This is what powers the table:

```text
Current Order Status
```

It always keeps the latest status for each order.

Example:

```text
ORD-ABCDE → DELIVERED
```

---

## 7. User clicks Trace Order

File:

```text
WorkflowController.java
```

Code:

```java
@GetMapping("/trace")
public String traceOrder(
        @RequestParam String orderId,
        Model model) {

    String cleanedOrderId =
            orderId.trim().toUpperCase();

    List<OrderTraceEvent> selectedTrace =
            orderTraceService.getTraceByOrderId(cleanedOrderId);

    model.addAttribute("traceMode", true);
    model.addAttribute("searchedOrderId", cleanedOrderId);
    model.addAttribute("selectedTrace", selectedTrace);
    model.addAttribute("selectedTraceCount", selectedTrace.size());

    model.addAttribute("orders", filteredOrders);
    model.addAttribute("allEvents", selectedTrace);

    return "workflow";
}
```

What this does:

```text
1. Takes orderId from search box.
2. Cleans spaces and converts to uppercase.
3. Searches traceStore by orderId.
4. Sends only that order's trace data to the UI.
5. Turns traceMode on.
```

---

## 8. Trace lookup method

File:

```text
OrderTraceService.java
```

Code:

```java
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
```

What this does:

```text
Find all trace events for one order ID.

Sort them by time.

Return them to the UI.
```

---

## 9. UI shows trace mode

File:

```text
workflow.html
```

Code:

```html
<div th:if="${traceMode}" class="trace-active">

    <h2>
        TRACE MODE ACTIVE:
        <span th:text="${searchedOrderId}"></span>
    </h2>

    <p>
        Trace records found:
        <b th:text="${selectedTraceCount}"></b>
    </p>

</div>
```

This confirms that tracing is active.

---

## 10. UI shows selected order timeline

File:

```text
workflow.html
```

Code:

```html
<tr th:each="event, iterStat : ${allEvents}">
    <td th:text="${iterStat.count}"></td>
    <td th:text="${event.orderId}"></td>
    <td th:text="${event.correlationId}"></td>
    <td th:text="${event.eventType}"></td>
    <td th:text="${event.status}"></td>
    <td th:text="${event.sourceService}"></td>
    <td th:text="${event.topic}"></td>
    <td th:text="${event.partition}"></td>
    <td th:text="${event.offset}"></td>
    <td th:text="${event.eventTime}"></td>
</tr>
```

In normal mode:

```text
allEvents = all workflow events
```

In trace mode:

```text
allEvents = selectedTrace
```

That is why the same table becomes filtered.

---

# Bucket 2 summary

```text
Every Kafka event carries orderId and correlationId.

Every consumer calls orderTraceService.addTrace().

OrderTraceService stores trace events in a Map.

When user searches orderId, controller fetches only that order's trace.

UI enters TRACE MODE and shows only that selected order timeline.
```


```text
Kafka stores the actual events in topics.

Our trace dashboard is a searchable application view.

In production, this trace view should be stored in a database or search index.
```

---

# Simple Explaination 

```text
Bucket 1: Sequencing

One topic triggers one consumer.
That consumer publishes the next event to the next topic.
The next consumer starts only when that next event appears.

Bucket 2: Tracing

Every event carries the same orderId/correlationId.
Every consumer records what it processed.
The trace UI searches by orderId and shows the event timeline.
```

That is the whole story of UI-13.
