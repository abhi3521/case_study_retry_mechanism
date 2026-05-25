package com.case_study.retry_case.service;

import com.case_study.retry_case.dto.Event;
import com.case_study.retry_case.dto.OrderEvent;
import com.case_study.retry_case.dto.Status;
import com.case_study.retry_case.entity.Retry;
import com.case_study.retry_case.repository.RetryRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;

@Slf4j
@Service
public class DeliveryService implements DeliveryInterface {
    @Autowired
    private RetryRepository retryRepository;

    @Autowired
    private ObjectMapper objectMapper;

    private final RestTemplate restTemplate = new RestTemplate();

    private static final String URL = "http://localhost:8081/orders";

    /**
     * Validates and delivers an event to microservice-2
     * @param event Event containing eventId, orderId and payload (JSON string of OrderEvent)
     */
    public void deliverEvent(Event event) {
        try {
            // Step 1: Validate Event object structure
            validateEventStructure(event);
            log.debug("Event structure validation passed for eventId: {}", event.getEventId());

            // Step 2: Parse payload JSON to OrderEvent object
            OrderEvent orderEvent = parsePayloadToOrderEvent(event);
            log.debug("Payload parsed successfully for eventId: {}", event.getEventId());

            // Step 3: Validate OrderEvent field values
            validateOrderEventFields(orderEvent, event.getEventId());
            log.debug("OrderEvent field validation passed for eventId: {}", event.getEventId());

            // Step 4: Validate eventId consistency
            validateEventIdConsistency(event, orderEvent);
            log.debug("EventId consistency validation passed for eventId: {}", event.getEventId());

            // Step 5: Send to microservice-2
            ResponseEntity<String> response = restTemplate.postForEntity(
                    URL,
                    orderEvent,
                    String.class
            );

            log.info("Delivered successfully: eventId={}, orderId={}, customerId={}, amount={}",
                    event.getEventId(),
                    orderEvent.getOrderId(),
                    orderEvent.getCustomerId(),
                    orderEvent.getAmount()
            );

        } catch (ValidationException ve) {
            log.warn("Validation failed for eventId: {}. Reason: {}", event.getEventId(), ve.getMessage());
            saveForRetry(event, Status.FAILED, ve.getMessage());

        } catch (Exception ex) {
            log.error("Delivery failed for eventId: {}. Error: {}", event.getEventId(), ex.getMessage());
            saveForRetry(event, Status.PENDING, ex.getMessage());
        }
    }

    /**
     * Validates the Event object has required fields
     */
    private void validateEventStructure(Event event) throws ValidationException {
        if (event == null) {
            throw new ValidationException("Event object is null");
        }
        if (event.getEventId() == null || event.getEventId().trim().isEmpty()) {
            throw new ValidationException("Event.eventId is missing or empty");
        }
        if (event.getOrderId() == null || event.getOrderId().trim().isEmpty()) {
            throw new ValidationException("Event.orderId is missing or empty");
        }
        if (event.getPayload() == null || event.getPayload().trim().isEmpty()) {
            throw new ValidationException("Event.payload is missing or empty");
        }
    }

    /**
     * Parses the JSON payload string to OrderEvent object
     */
    private OrderEvent parsePayloadToOrderEvent(Event event) throws ValidationException {
        try {
            OrderEvent orderEvent = objectMapper.readValue(event.getPayload(), OrderEvent.class);
            return orderEvent;
        } catch (Exception e) {
            throw new ValidationException("Failed to parse payload JSON to OrderEvent: " + e.getMessage(), e);
        }
    }

    /**
     * Validates OrderEvent fields are not null and have valid values
     */
    private void validateOrderEventFields(OrderEvent orderEvent, String eventId) throws ValidationException {
        if (orderEvent == null) {
            throw new ValidationException("OrderEvent object is null after parsing");
        }

        if (orderEvent.getEventId() == null || orderEvent.getEventId().trim().isEmpty()) {
            throw new ValidationException("OrderEvent.eventId is missing or empty");
        }

        if (orderEvent.getOrderId() == null || orderEvent.getOrderId().trim().isEmpty()) {
            throw new ValidationException("OrderEvent.orderId is missing or empty");
        }

        if (orderEvent.getCustomerId() == null || orderEvent.getCustomerId().trim().isEmpty()) {
            throw new ValidationException("OrderEvent.customerId is missing or empty");
        }

        if (orderEvent.getAmount() == null) {
            throw new ValidationException("OrderEvent.amount is null");
        }

        if (orderEvent.getAmount() <= 0) {
            throw new ValidationException("OrderEvent.amount must be greater than 0, got: " + orderEvent.getAmount());
        }

        if (orderEvent.getStatus() == null || orderEvent.getStatus().trim().isEmpty()) {
            throw new ValidationException("OrderEvent.status is missing or empty");
        }
    }

    /**
     * Validates that eventId in Event matches eventId in OrderEvent payload
     */
    private void validateEventIdConsistency(Event event, OrderEvent orderEvent) throws ValidationException {
        if (!event.getEventId().equals(orderEvent.getEventId())) {
            throw new ValidationException(
                    String.format("EventId mismatch: Event.eventId='%s' but OrderEvent.eventId='%s'",
                            event.getEventId(),
                            orderEvent.getEventId())
            );
        }
    }

    /**
     * Saves failed events to retry table for later processing
     */
    private void saveForRetry(Event event, Status status, String errorMessage) {
        try {
            Retry retry = Retry.builder()
                    .eventId(event.getEventId())
                    .payload(objectMapper.writeValueAsString(event))
                    .status(status)
                    .retryCount(0)
                    .createdAt(LocalDateTime.now())
                    .nextRetryAt(LocalDateTime.now().plusSeconds(10))
                    .build();

            retryRepository.save(retry);
            log.info("Saved {} event with eventId: {} - Reason: {}",
                    status.name(),
                    retry.getEventId(),
                    errorMessage
            );

        } catch (Exception e) {
            log.error("Failed to save retry event for eventId: {}. Error: {}",
                    event.getEventId(),
                    e.getMessage()
            );
        }
    }

    /**
     * Custom exception for validation failures
     */
    public static class ValidationException extends Exception {
        public ValidationException(String message) {
            super(message);
        }

        public ValidationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
