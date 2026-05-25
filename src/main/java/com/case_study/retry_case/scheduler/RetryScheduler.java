package com.case_study.retry_case.scheduler;

import com.case_study.retry_case.dto.Event;
import com.case_study.retry_case.dto.Status;
import com.case_study.retry_case.entity.Retry;
import com.case_study.retry_case.repository.RetryRepository;
import com.case_study.retry_case.service.DeliveryInterface;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.List;
@Slf4j
@Component
public class RetryScheduler {
    @Autowired
    private RetryRepository retryRepository;

    @Autowired
    private DeliveryInterface orderDelivery;

    @Autowired
    private ObjectMapper objectMapper;

    private final RestTemplate restTemplate = new RestTemplate();

    private static final String URL =
            "http://localhost:8081/orders";

    @Scheduled(fixedDelay = 10000)
    public void retryFailedMessages() {

        log.info("Scheduler running...");

        List<Retry> retries =
                retryRepository.findByStatusAndNextRetryAtBefore(
                        Status.PENDING,
                        LocalDateTime.now()
                );

        for (Retry retry : retries) {

            try {

                Event event =
                        objectMapper.readValue(
                                retry.getPayload(),
                                Event.class
                        );

                restTemplate.postForEntity(
                        URL,
                        event,
                        Void.class
                );

                retry.setStatus(Status.SUCCESS);

                retryRepository.save(retry);

                log.info("Retry Success: {}", event.getEventId());

            } catch (Exception ex) {

                retry.setRetryCount(retry.getRetryCount() + 1);

                retry.setNextRetryAt(LocalDateTime.now().plusSeconds(10));

                retryRepository.save(retry);
                log.error("Retry Failed for eventId: {}. Error: {}",
                        retry.getEventId(),
                        ex.getMessage()
                );
            }
        }
    }
}
