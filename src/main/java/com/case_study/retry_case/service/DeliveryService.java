package com.case_study.retry_case.service;

import com.case_study.retry_case.dto.Event;
import com.case_study.retry_case.dto.Status;
import com.case_study.retry_case.entity.Retry;
import com.case_study.retry_case.repository.RetryRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
@Slf4j
@Service
public class DeliveryService implements DeliveryInterface{
    @Autowired
    private RetryRepository retryRepository;

    @Autowired
    private ObjectMapper objectMapper;

    private final RestTemplate restTemplate = new RestTemplate();

    private static final String URL =
            "http://localhost:8081/orders";

    public void deliverEvent(Event event) {

        try {

            ResponseEntity<Void> response =
                    restTemplate.postForEntity(
                            URL,
                            event,
                            Void.class
                    );

            log.info("Delivered successfully: {}", event.getEventId());

        } catch (Exception ex) {
            log.error("Delivery failed for: {}. Error: {}",
                    event.getEventId(),
                    ex.getMessage()
            );

            saveForRetry(event);
        }
    }

    private void saveForRetry(Event event) {

        try {

            Retry retry = Retry.builder()
                    .eventId(event.getEventId())
                    .payload(objectMapper.writeValueAsString(event))
                    .status(Status.PENDING)
                    .retryCount(0)
                    .createdAt(LocalDateTime.now())
                    .nextRetryAt(LocalDateTime.now().plusSeconds(10))
                    .build();

            retryRepository.save(retry);
            log.info("Saved retry event: {} with payload: {}",
                    retry.getEventId(),
                    retry.getPayload()
            );

        } catch (Exception e) {
            log.error("Failed to save retry event: {}. Error: {}",
                    event.getEventId(),
                    e.getMessage()
            );

            e.printStackTrace();
        }
    }
}
