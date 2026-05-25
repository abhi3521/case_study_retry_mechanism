package com.case_study.retry_case.validator;

import com.case_study.retry_case.dto.Event;
import com.case_study.retry_case.dto.OrderEvent;
import com.case_study.retry_case.exception.PayloadValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Validator class to validate event payload against OrderEvent schema.
 * Ensures that the payload contains all required fields with valid data types.
 */
@Slf4j
@Component
public class PayloadValidator {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Validates the event payload against OrderEvent structure.
     *
     * @param event The Event object containing the payload to validate
     * @return OrderEvent The deserialized and validated OrderEvent object
     * @throws PayloadValidationException if payload is invalid or missing required fields
     */
    public OrderEvent validateAndDeserialize(Event event) {

        if (event == null) {
            throw new PayloadValidationException("Event cannot be null");
        }

        String payload = event.getPayload();

        if (payload == null || payload.trim().isEmpty()) {
            throw new PayloadValidationException(
                    "Event payload is empty or null for eventId: " + event.getEventId()
            );
        }

        try {
            // Deserialize payload to OrderEvent
            OrderEvent orderEvent = objectMapper.readValue(payload, OrderEvent.class);

            // Validate required fields
            validateOrderEventFields(orderEvent);

            log.info("Payload validation successful for eventId: {}", event.getEventId());
            return orderEvent;

        } catch (JacksonException ex) {
            log.error("Unexpected error during payload validation for eventId: {}",
                    event.getEventId(), ex);
            throw new PayloadValidationException(
                    "Unexpected error during payload validation: " + ex.getMessage(),
                    ex
            );
        }
    }

    /**
     * Validates that all required fields in OrderEvent are present and valid.
     *
     * @param orderEvent The OrderEvent to validate
     * @throws PayloadValidationException if any required field is missing or invalid
     */
    private void validateOrderEventFields(OrderEvent orderEvent) {

        if (orderEvent.getEventId() == null || orderEvent.getEventId().trim().isEmpty()) {
            throw new PayloadValidationException("OrderEvent field 'eventId' is missing or empty");
        }

        if (orderEvent.getOrderId() == null || orderEvent.getOrderId().trim().isEmpty()) {
            throw new PayloadValidationException("OrderEvent field 'orderId' is missing or empty");
        }

        if (orderEvent.getCustomerId() == null || orderEvent.getCustomerId().trim().isEmpty()) {
            throw new PayloadValidationException("OrderEvent field 'customerId' is missing or empty");
        }

        if (orderEvent.getAmount() == null) {
            throw new PayloadValidationException("OrderEvent field 'amount' is missing");
        }

        if (orderEvent.getAmount() <= 0) {
            throw new PayloadValidationException("OrderEvent field 'amount' must be greater than zero");
        }

        if (orderEvent.getStatus() == null || orderEvent.getStatus().trim().isEmpty()) {
            throw new PayloadValidationException("OrderEvent field 'status' is missing or empty");
        }
    }
}
