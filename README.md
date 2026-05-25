# Microservice-1: Event Publisher & Retry Mechanism (MS1)

## 📋 Overview

**MS1** is an event publishing microservice that handles asynchronous order event distribution with a robust **automatic retry mechanism**. It acts as a producer that publishes events through Kafka and ensures delivery to downstream microservices (MS2) using intelligent retry logic and event persistence.

**Port:** `8080`
**Language:** Java 17
**Framework:** Spring Boot 4.0.6

---

## 🎯 Key Features

### 1. **Event Publishing**
- REST API to publish order events
- Kafka integration for asynchronous message handling
- JSON payload validation and transformation

### 2. **Automatic Retry Mechanism**
- Failed deliveries are automatically persisted to the database
- Scheduled retry attempts with exponential backoff
- Configurable retry intervals and maximum retry counts
- Detailed audit trail for all retry attempts

### 3. **Payload Validation**
- Validates `Event` structure (eventId, orderId, payload)
- Parses and validates embedded OrderEvent JSON payload
- Ensures data type consistency (String, Number, etc.)
- Validates business rules (amount > 0, valid status values)
- EventId consistency checks between Event wrapper and OrderEvent payload

### 4. **Event Persistence**
- Stores failed events with metadata
- Supports audit and debugging
- Tracks retry count and next retry timestamp

---

## 🏗️ Architecture

### Component Structure

```
MS1 (Event Publisher & Retry Mechanism)
│
├── Controller Layer
│   └── PublishController
│       └── POST /publish
│
├── Service Layer
│   ├── ProducerService (Kafka publishing)
│   ├── DeliveryService (Direct HTTP delivery + retry logic)
│   └── RetryScheduler (Scheduled retry attempts)
│
├── Data Layer
│   ├── RetryRepository (JPA repository)
│   └── Retry Entity
│
├── DTO Layer
│   ├── Event (Event wrapper)
│   ├── OrderEvent (Order payload)
│   └── Status Enum
│
└── Configuration
    └── JacksonConfig (ObjectMapper bean)
```

### Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. REST API Request to /publish with Event payload             │
│    {                                                             │
│      "eventId": "evt-001",                                      │
│      "orderId": "ord-12345",                                    │
│      "payload": "{JSON OrderEvent string}"                      │
│    }                                                             │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │ Validation Pipeline  │
            ├──────────────────────┤
            │ 1. Event structure   │
            │ 2. JSON parsing      │
            │ 3. Field validation  │
            │ 4. EventId match     │
            │ 5. Business rules    │
            └──────────┬───────────┘
                       │
         ┌─────────────┴─────────────┐
         │ Valid                      │ Invalid
         ▼                            ▼
    ┌────────────┐          ┌─────────────────┐
    │ Kafka      │          │ Save to Retry   │
    │ Producer   │          │ with ERROR      │
    │ (PENDING)  │          │ status          │
    └─────┬──────┘          └─────────────────┘
          │
          ▼
    ┌────────────┐
    │ Kafka      │
    │ Consumer   │
    └─────┬──────┘
          │
          ▼
    ┌────────────────────────────────┐
    │ DeliveryService                │
    │ Attempts HTTP POST to MS2      │
    │ (http://localhost:8081/orders) │
    └────┬─────────────────────────┬─┘
         │                         │
     Success                   Failure
         │                         │
         ▼                         ▼
    ┌──────────┐          ┌─────────────────┐
    │ Log      │          │ Save to Retry   │
    │ Success  │          │ with PENDING    │
    │          │          │ nextRetryAt += 10s
    └──────────┘          └────────┬────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │ Retry Scheduler             │
                    │ (Runs every 5 seconds)      │
                    │ Retries PENDING events      │
                    └─────────────────────────────┘
```

---

## 🗄️ Database Schema

### Retry Table

```sql
CREATE TABLE retry (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_id VARCHAR(255) NOT NULL UNIQUE,
    payload LONGTEXT NOT NULL,
    status VARCHAR(50) NOT NULL,        -- PENDING, SUCCESS, FAILED, VALIDATION_FAILED
    retry_count INT DEFAULT 0,
    next_retry_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

---

## 🔧 Configuration

### application.properties

```properties
# Server
spring.application.name=retry_case
server.port=8080

# Database
spring.datasource.url=jdbc:mysql://localhost:3306/retry_db
spring.datasource.username=root
spring.datasource.password=your_password
spring.jpa.hibernate.ddl-auto=update
spring.jpa.show-sql=false

# Kafka
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
spring.kafka.consumer.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=order-consumer-group
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.trusted.packages=*

# Logging
logging.level.com.case_study=DEBUG
logging.level.org.springframework.kafka=INFO
```

---

## 📡 API Endpoints

### 1. Publish Event

**Endpoint:** `POST /publish`

**Request Body:**
```json
{
  "eventId": "evt-001",
  "orderId": "ord-12345",
  "payload": "{\"eventId\":\"evt-001\",\"orderId\":\"ord-12345\",\"customerId\":\"cust-789\",\"amount\":499.99,\"status\":\"PENDING\"}"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Event Published",
  "timestamp": "2026-05-26T04:30:00Z"
}
```

**Status Codes:**
- `200 OK` - Event published successfully
- `400 Bad Request` - Validation failed
- `500 Internal Server Error` - Server error

---

## 🔄 Retry Logic

### Retry Configuration

| Parameter | Value | Description |
|-----------|-------|-------------|
| **Initial Delay** | 10 seconds | First retry after 10s |
| **Scheduler Interval** | 5 seconds | Scheduler checks every 5s |
| **Max Retries** | 5 | Maximum 5 retry attempts |
| **Backoff Strategy** | Linear (5s + count) | 10s, 20s, 30s, 40s, 50s |

### Retry States

```
┌─────────────┐
│ VALIDATION  │ ← Payload validation failed
│ FAILED      │   (Manual intervention needed)
└─────────────┘

┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   PENDING   │ ──→  │  DELIVERY   │ ──→  │  SUCCESS    │
│ (Retrying)  │      │  FAILED     │      │  (Delivered)│
└─────────────┘      └─────────────┘      └─────────────┘
      │                      │
      │          Retry exhausted
      └──────────────────────┘
              │
              ▼
         ┌─────────────┐
         │   FAILED    │
         │ (Alert/DLQ) │
         └─────────────┘
```

---

## 🧪 Testing

### Test Payload (Valid)

```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "evt-001",
    "orderId": "ord-12345",
    "payload": "{\"eventId\":\"evt-001\",\"orderId\":\"ord-12345\",\"customerId\":\"cust-789\",\"amount\":499.99,\"status\":\"PENDING\"}"
  }'
```

### Test Payloads (Invalid)

**Missing customerId:**
```json
{
  "eventId": "evt-002",
  "orderId": "ord-12346",
  "payload": "{\"eventId\":\"evt-002\",\"orderId\":\"ord-12346\",\"amount\":299.99,\"status\":\"PENDING\"}"
}
```

**Invalid amount (string instead of number):**
```json
{
  "eventId": "evt-003",
  "orderId": "ord-12347",
  "payload": "{\"eventId\":\"evt-003\",\"orderId\":\"ord-12347\",\"customerId\":\"cust-790\",\"amount\":\"299.99\",\"status\":\"PENDING\"}"
}
```

**EventId mismatch:**
```json
{
  "eventId": "evt-004",
  "orderId": "ord-12348",
  "payload": "{\"eventId\":\"evt-999\",\"orderId\":\"ord-12348\",\"customerId\":\"cust-791\",\"amount\":399.99,\"status\":\"PENDING\"}"
}
```

---

## 📊 Monitoring & Logs

### Key Logs to Monitor

```
✅ Delivery successful
2026-05-26T04:30:15.123 INFO  DeliveryService - Delivered successfully: evt-001

❌ Validation failed
2026-05-26T04:30:20.456 ERROR PayloadValidator - Validation failed for evt-002: Missing field 'customerId'

🔄 Retry scheduled
2026-05-26T04:30:25.789 INFO  RetryScheduler - Retrying evt-001, attempt 1/5

💾 Event saved for retry
2026-05-26T04:30:30.012 INFO  DeliveryService - Saved retry event: evt-001 with payload
```

### Database Audit Query

```sql
-- View all retry events
SELECT event_id, status, retry_count, next_retry_at, created_at 
FROM retry 
ORDER BY created_at DESC 
LIMIT 20;

-- Check pending retries
SELECT * FROM retry 
WHERE status = 'PENDING' AND next_retry_at <= NOW();

-- View failed events
SELECT * FROM retry 
WHERE status = 'FAILED' 
ORDER BY created_at DESC;
```

---

## 🚀 Setup & Deployment

### Prerequisites

- Java 17+
- MySQL 8.0+
- Apache Kafka 3.0+
- Maven 3.6+

### Local Development

1. **Clone repository:**
   ```bash
   git clone https://github.com/abhi3521/case_study_retry_mechanism_ms1.git
   cd case_study_retry_mechanism_ms1
   ```

2. **Setup database:**
   ```bash
   mysql -u root -p
   CREATE DATABASE retry_db;
   USE retry_db;
   -- Tables will be created by Hibernate DDL
   ```

3. **Start Docker services:**
   ```bash
   docker-compose up -d
   ```

4. **Build and run:**
   ```bash
   mvn clean install
   mvn spring-boot:run
   ```

5. **Verify startup:**
   ```
   Application started on http://localhost:8080
   Swagger UI: http://localhost:8080/swagger-ui.html
   ```

---

## 🔗 Integration with MS2

MS1 sends validated orders to MS2 at:
```
POST http://localhost:8081/orders
```

**Payload sent to MS2:**
```json
{
  "eventId": "evt-001",
  "orderId": "ord-12345",
  "customerId": "cust-789",
  "amount": 499.99,
  "status": "PENDING"
}
```

---

## 📝 Key Classes

| Class | Purpose |
|-------|---------|
| `PublishController` | REST API endpoint for publishing events |
| `ProducerService` | Kafka producer for event publishing |
| `DeliveryService` | HTTP delivery and retry orchestration |
| `RetryScheduler` | Scheduled retry attempts |
| `PayloadValidator` | Comprehensive payload validation |
| `Retry` (Entity) | Event persistence for retries |
| `RetryRepository` | JPA data access layer |
| `Event` (DTO) | Event wrapper structure |
| `OrderEvent` (DTO) | Order payload structure |

---

## 🐛 Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| ObjectMapper bean not found | Missing JacksonConfig | Create JacksonConfig with @Configuration and @Bean |
| Null amount error in MS2 | Event object sent instead of OrderEvent | DeliveryService now parses payload JSON |
| Duplicate events | Missing eventId unique constraint | Add UNIQUE constraint to retry table |
| Kafka not connecting | Kafka not running | `docker-compose up -d` and verify bootstrap servers |
| MySQL connection failed | Wrong credentials/host | Check application.properties database config |

---

## 📚 Related Documentation

- [Approach Document](./APPROACH.md) - Detailed implementation strategy
- [MS2 Integration](../case_study_retry_mechanism_ms2/README.md) - Order service documentation

---

## 📞 Support

For issues, questions, or contributions, please create an issue in the repository.

---

**Version:** 1.0.0  
**Last Updated:** 2026-05-26  
**Author:** Case Study Team
