# Architecture & Implementation Approach for MS1

## 📌 Overview

This document provides a detailed explanation of the architectural decisions, design patterns, and implementation strategies used in the Microservice-1 (Event Publisher & Retry Mechanism).

---

## 🎯 Problem Statement

**Challenge:**
When publishing events from MS1 to downstream services (MS2), network failures or service unavailability can cause events to be lost. We need a mechanism to:

1. Automatically retry failed deliveries
2. Persist failed events for audit and manual recovery
3. Validate event payload structure before processing
4. Ensure exactly-once delivery semantics (idempotency)
5. Provide observable and debuggable retry attempts

---

## 🏗️ Architectural Design

### 1. Event Publishing Pattern

We use a **Publish-Subscribe** pattern with Kafka as the message broker:

```
┌──────────────┐
│   Client     │
└──────┬───────┘
       │ REST API
       ▼
┌──────────────────┐      ┌──────────┐
│ PublishController│ ───→ │  Kafka   │
└──────────────────┘      │  Topic   │
                          └────┬─────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
                    ▼                     ▼
            ┌───────────────┐    ┌──────────────┐
            │   Consumer    │    │   Consumer   │
            │  (Delivery)   │    │  (Analytics) │
            └───────────────┘    └──────────────┘
```

**Benefits:**
- **Decoupling:** Publishers and consumers are independent
- **Scalability:** Easy to add new consumers
- **Reliability:** Message persistence in Kafka
- **Ordering:** Guaranteed order per partition

---

### 2. Retry Strategy

#### A. Exponential Backoff with Linear Increment

```
Attempt 1: 10 seconds     ┐
Attempt 2: 20 seconds     │
Attempt 3: 30 seconds     ├─ Linear backoff
Attempt 4: 40 seconds     │ (5s + retry_count * 5s)
Attempt 5: 50 seconds     ┘
```

**Why Linear Backoff?**
- Simpler to understand and debug than exponential
- Prevents overwhelming downstream services
- Provides reasonable retry window (50s max)

#### B. Scheduled Retry Executor

```
┌─────────────────────────────────────┐
│ RetryScheduler (runs every 5 secs)  │
│                                     │
│ 1. Query DB: status='PENDING'       │
│    AND next_retry_at <= NOW()       │
│                                     │
│ 2. For each event:                  │
│    ├─ Increment retry_count         │
│    ├─ Call DeliveryService          │
│    ├─ Update status                 │
│    └─ Set next_retry_at             │
│                                     │
│ 3. Max 5 retries per event          │
└─────────────────────────────────────┘
```

**Implementation Details:**
- Uses `@Scheduled(fixedDelay = 5000)`
- Transactional to prevent race conditions
- Pessimistic locking prevents duplicate retries
- Failed retries are scheduled again

---

### 3. Payload Validation Pipeline

#### 3-Layer Validation Architecture

```
Layer 1: Structure Validation
├─ Event object exists
├─ eventId not null/empty
├─ orderId not null/empty
└─ payload not null/empty
         │
         ▼
Layer 2: Payload Parsing & Schema Validation
├─ Parse JSON payload string
├─ Extract OrderEvent object
├─ Validate all OrderEvent fields exist
├─ Validate data types (String, Number, etc.)
└─ Check for unexpected fields
         │
         ▼
Layer 3: Business Rules Validation
├─ amount > 0
├─ status in [PENDING, CONFIRMED, CANCELLED]
├─ eventId matches between Event and OrderEvent
└─ customerId format validation
         │
         ▼
    Valid ✅ / Invalid ❌
```

#### Implementation Class: PayloadValidator

```java
public class PayloadValidator {
    
    /**
     * 5-step validation process
     */
    public ValidationResult validate(Event event) {
        // Step 1: Event structure
        if (!validateEventStructure(event)) {
            return ValidationResult.FAILED("Invalid event structure");
        }
        
        // Step 2: Parse and extract OrderEvent
        OrderEvent orderEvent = parseOrderEvent(event.getPayload());
        if (orderEvent == null) {
            return ValidationResult.FAILED("Invalid JSON payload");
        }
        
        // Step 3: OrderEvent field validation
        if (!validateOrderEventFields(orderEvent)) {
            return ValidationResult.FAILED("Missing or invalid OrderEvent fields");
        }
        
        // Step 4: EventId consistency
        if (!event.getEventId().equals(orderEvent.getEventId())) {
            return ValidationResult.FAILED("EventId mismatch");
        }
        
        // Step 5: Business rules
        if (!validateBusinessRules(orderEvent)) {
            return ValidationResult.FAILED("Business rule violation");
        }
        
        return ValidationResult.SUCCESS(orderEvent);
    }
}
```

---

### 4. Delivery Service Architecture

#### Dual Delivery Modes

MS1 supports two delivery mechanisms:

##### Mode 1: Kafka Consumer → Direct HTTP Delivery

```
┌──────────────────────┐
│ Kafka Consumer       │
│ (ConsumerService)    │
└──────────┬───────────┘
           │ Message from Kafka
           ▼
┌──────────────────────┐
│ DeliveryService      │
│ (HTTP POST to MS2)   │
└──────────┬───────────┘
           │
      ┌────┴────┐
      │          │
   Success    Failure
      │          │
      ▼          ▼
   Log      Save to Retry DB
   ✅        with PENDING status
```

##### Mode 2: Direct REST API → Kafka → Consumer

```
Client
  │ REST POST /publish
  ▼
PublishController
  │ Validates
  ▼
ProducerService
  │ Sends to Kafka
  ▼
Kafka Topic
  │
Consumer picks up
  │
DeliveryService
```

---

### 5. State Machine Design

#### Event Status Lifecycle

```
         ┌──────────────────────┐
         │  VALIDATION_FAILED   │ ← Invalid payload detected
         │  (Terminal State)    │   Requires manual intervention
         └──────────────────────┘

         ┌──────────────────────┐
    ┌───→│  PENDING             │ ← Event saved for retry
    │    │  (Retrying)          │   Scheduled retry active
    │    └──────────┬───────────┘
    │               │ Retry attempt
    │               ▼
    │    ┌──────────────────────┐
    │    │  DELIVERY ATTEMPT    │ ← Transient state
    │    │                      │   (HTTP call in progress)
    │    └──────────┬───────────┘
    │               │
    │         ┌─────┴─────┐
    │         │           │
    │      Success     Failure
    │         │           │
    │         ▼           ▼
    │    ┌─────────┐  ┌─────────┐
    │    │ SUCCESS │  │ Retry?  │
    │    │ (Done)  │  │         │
    │    └─────────┘  └────┬────┘
    │                      │
    │         ┌────────────┴─────────────┐
    │         │ Retry count < Max?       │
    │         ├─────────────┬────────────┤
    │         │ Yes (≤5)    │ No (>5)    │
    │         ▼             ▼
    │    ┌──────────┐  ┌──────────┐
    │    │ PENDING  │  │ FAILED   │
    │    │          │  │ (Alert)  │
    │    └──────────┘  └──────────┘
    │         │
    └─────────┘
```

---

### 6. Data Persistence Strategy

#### Retry Table Design

```sql
CREATE TABLE retry (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    
    -- Event Identity
    event_id VARCHAR(255) NOT NULL UNIQUE,
    order_id VARCHAR(255),
    
    -- Event Data
    payload LONGTEXT NOT NULL,           -- Full Event JSON
    
    -- Retry Tracking
    status VARCHAR(50) NOT NULL,         -- PENDING, SUCCESS, FAILED, VALIDATION_FAILED
    retry_count INT DEFAULT 0,           -- 0 to 5
    next_retry_at TIMESTAMP,             -- When to retry next
    
    -- Audit Trail
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicate events
    UNIQUE KEY uk_event_id (event_id),
    INDEX idx_status_retry (status, next_retry_at)
);
```

**Design Decisions:**

| Decision | Reason |
|----------|--------|
| **eventId UNIQUE** | Prevents duplicate processing (idempotency) |
| **Composite Index** | Fast query for `SELECT * WHERE status='PENDING' AND next_retry_at <= NOW()` |
| **LONGTEXT for payload** | Supports large JSON documents |
| **created_at/updated_at** | Audit trail and debugging |

---

### 7. Concurrency & Thread Safety

#### Problem: Race Conditions in Retry

```
Scenario: Two scheduler threads pick same event
┌────────────────────────────────────────────┐
│ Scheduler Thread 1      Scheduler Thread 2 │
│                                            │
│ 1. Query evt-001        1. Query evt-001   │
│    (PENDING)               (PENDING)       │
│                                            │
│ 2. Update retry_count=1 2. Update retry_count=1
│    (Both execute!)                         │
│                                            │
│ Result: Duplicate delivery ❌              │
└────────────────────────────────────────────┘
```

#### Solution: Pessimistic Locking

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT r FROM Retry r WHERE r.status = 'PENDING' AND r.nextRetryAt <= CURRENT_TIMESTAMP")
List<Retry> findPendingRetries();
```

**How it works:**
1. Database acquires write lock on rows
2. Only one transaction can update simultaneously
3. Other transactions wait for lock release
4. Prevents race condition

---

### 8. Error Handling Strategy

#### Cascading Error Recovery

```
Level 1: Validation Error
├─ Invalid JSON → VALIDATION_FAILED (no retry)
├─ Missing field → VALIDATION_FAILED (no retry)
└─ Type mismatch → VALIDATION_FAILED (no retry)

Level 2: Network Error
├─ Connection timeout → PENDING (retry scheduled)
├─ Socket exception → PENDING (retry scheduled)
└─ Unknown host → PENDING (retry scheduled)

Level 3: Application Error
├─ 4xx response → VALIDATION_FAILED (no retry)
├─ 5xx response → PENDING (retry scheduled)
└─ Null response → PENDING (retry scheduled)

Level 4: System Error
├─ Database down → Log + Alert
├─ Kafka unavailable → Log + Alert
└─ Thread exception → Log + Alert
```

---

## 🔄 Integration Points

### MS1 → MS2 Communication

```
MS1 (Retry Mechanism)              MS2 (Order Service)
        │                                  │
        │ 1. Publishes Event              │
        │ to Kafka topic                  │
        ├─────────────────────────────────┤
        │                                  │
        │ 2. Consumer picks up            │
        │ message from Kafka              │
        │                                  │
        │ 3. DeliveryService validates    │
        │ and extracts OrderEvent         │
        │                                  │
        │ 4. HTTP POST to /orders         │
        │──────────────────────────────────→ 5. Receives OrderEvent
        │                                  │
        │ 6. MS2 processes and saves      │
        │ order to database               │
        │                                  │
        │←─────────────────────────────────── 7. Returns 200 OK
        │                                  │
        │ 8. DeliveryService marks        │
        │ as SUCCESS                      │
        │                                  │
        └──────────────────────────────────┘
```

**Protocol Details:**
- **Endpoint:** `POST http://localhost:8081/orders`
- **Content-Type:** `application/json`
- **Authentication:** None (can be extended)
- **Timeout:** 5 seconds (configurable)

---

## 📊 Performance Characteristics

### Throughput Analysis

```
Scenario 1: All events succeed
├─ Latency: ~100-200ms per event (HTTP + DB)
├─ Throughput: ~5-10 events/second (single thread)
└─ Can scale horizontally with multiple instances

Scenario 2: 10% failure rate with retries
├─ Failed events wait 10-50 seconds
├─ Retry scheduler processes 5-10 retries/batch
├─ Average delivery success: ~99.9% (after 5 retries)
└─ Total time to success: 5 minutes per event (worst case)
```

### Database Impact

```
INSERT on publish failure:      1 row
UPDATE on retry attempt:        1 row/attempt
SELECT for retry discovery:     1 query/5 seconds

Storage:
├─ 1000 events: ~1 MB (avg payload ~1KB)
├─ 100,000 events: ~100 MB
└─ Archive after 30 days recommended
```

---

## 🔐 Idempotency & Exactly-Once Delivery

### Problem: Duplicate Processing

```
Scenario: Network timeout on successful delivery
┌────────────────────────────────────────┐
│ MS1 sends order evt-001 to MS2        │
├────────────────────────────────────────┤
│ MS2 receives and saves order          │
├────────────────────────────────────────┤
│ Network timeout (no 200 OK response)   │
├────────────────────────────────────────┤
│ MS1 thinks delivery failed             │
├────────────────────────────────────────┤
│ Retry scheduler sends evt-001 again    │
├────────────────────────────────────────┤
│ MS2 receives duplicate order!          │
│ Result: Two identical orders ❌        │
└────────────────────────────────────────┘
```

### Solution: Idempotency Key

```java
// MS2 uses eventId as idempotency key
public Order addOrder(OrderEvent event) {
    // Check if order with this eventId already exists
    Optional<Order> existing = orderRepository.findByEventId(event.getEventId());
    
    if (existing.isPresent()) {
        // Return existing order (idempotent)
        return existing.get();
    }
    
    // Create new order
    Order order = new Order(event);
    return orderRepository.save(order);
}
```

**Implementation:**
- `eventId` as UNIQUE constraint in MS2
- First request creates order
- Retry requests return existing order
- Result: Exactly-once delivery semantics ✅

---

## 🚀 Scalability & Extension Points

### Horizontal Scaling

```
┌──────────────┐
│   Load       │
│ Balancer     │
└──────┬───────┘
       │
    ┌──┴──┬──────┬──────┐
    │     │      │      │
    ▼     ▼      ▼      ▼
┌──────┬───┬──────┬───┬──────┬───┐
│ MS1  │   │ MS1  │   │ MS1  │   │
│ Pod1 │ ... Pod2 │ ... Pod3 │ ...
└──────┴───┴──────┴───┴──────┴───┘
    │     │      │      │
    └──────┼──────┼──────┘
           │      │
        ┌──┴──────┴──┐
        │ Shared     │
        │ MySQL DB   │
        │ Kafka      │
        └────────────┘
```

**Scaling Strategies:**
1. **Database Replication:** Master-Slave for read distribution
2. **Kafka Partitions:** Multiple consumer threads per partition
3. **Caching:** Redis for frequency checks (advanced)
4. **Circuit Breaker:** Fallback if MS2 unavailable

---

## 📈 Monitoring & Observability

### Key Metrics to Track

```
1. Event Publishing Rate
   - Events/second
   - Peak load analysis

2. Delivery Success Rate
   - First-attempt success %
   - Final success % (after retries)
   - Failure rate

3. Retry Metrics
   - Pending events count
   - Avg retries per event
   - Max retry time

4. Performance Metrics
   - P50, P95, P99 latencies
   - HTTP response times
   - Database query times

5. Error Metrics
   - Validation failures
   - Network timeouts
   - Database errors
```

### Example Prometheus Queries

```promql
# Event delivery success rate
rate(delivery_success_total[5m]) / rate(delivery_total[5m]) * 100

# Pending retry count
SELECT COUNT(*) FROM retry WHERE status = 'PENDING'

# Avg retry count per event
SELECT AVG(retry_count) FROM retry WHERE status = 'SUCCESS'
```

---

## 🔄 Comparison with Alternatives

### Alternative 1: Synchronous HTTP Retries (Not Chosen)

```
Pros:
✅ Simple to implement
✅ No message broker needed

Cons:
❌ Blocks client thread during retries
❌ Poor performance under load
❌ No event persistence
❌ Lost on service restart
```

### Alternative 2: Message Queue Only (Not Chosen)

```
Pros:
✅ Guaranteed message delivery
✅ Good decoupling

Cons:
❌ No retry control (Queue holds forever)
❌ No visibility into failures
❌ Complex recovery
❌ No explicit backoff strategy
```

### Chosen: Kafka + Database Retry (✅ Best Fit)

```
Pros:
✅ Event streaming capability
✅ Explicit retry control
✅ Full visibility/auditability
✅ Idempotency guarantee
✅ Configurable backoff
✅ Failed event recovery

Cons:
⚠️  More complex
⚠️  Requires two storage systems
```

---

## 🎓 Design Patterns Used

| Pattern | Implementation | Purpose |
|---------|-----------------|---------|
| **Publish-Subscribe** | Kafka + Topic | Decoupling publishers & consumers |
| **Circuit Breaker** | Could be added | Prevent cascading failures |
| **Retry** | RetryScheduler | Transient failure recovery |
| **Idempotency Key** | eventId | Exactly-once delivery |
| **Repository** | RetryRepository | Data access abstraction |
| **Service Layer** | DeliveryService | Business logic encapsulation |
| **Validator** | PayloadValidator | Input validation |
| **Observer/Scheduled** | @Scheduled | Polling-based retry detection |

---

## 🔮 Future Enhancements

1. **Dead Letter Queue (DLQ)**
   - Move permanently failed events to separate queue
   - Enable manual reprocessing

2. **Circuit Breaker Pattern**
   - Detect failing services early
   - Prevent cascading failures

3. **Distributed Tracing**
   - Correlation IDs across MS1 ↔ MS2
   - End-to-end latency tracking

4. **Event Versioning**
   - Handle schema evolution
   - Backward compatibility

5. **Metrics & Alerting**
   - Prometheus integration
   - PagerDuty/Slack alerts

6. **Admin Dashboard**
   - Retry status visualization
   - Manual retry trigger
   - Event exploration

---

## 📚 References

- [Spring Kafka Documentation](https://spring.io/projects/spring-kafka)
- [JPA/Hibernate Locking](https://docs.jboss.org/hibernate/orm/6.0/userguide/html_single/)
- [Exactly-Once Semantics](https://kafka.apache.org/documentation/#semantics)
- [Exponential Backoff Strategy](https://aws.amazon.com/blogs/architecture/)

---

**Document Version:** 1.0.0  
**Last Updated:** 2026-05-26  
**Author:** Case Study Architecture Team
