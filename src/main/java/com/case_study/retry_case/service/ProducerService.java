package com.case_study.retry_case.service;

import com.case_study.retry_case.dto.Event;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ProducerService {
    private static final String TOPIC = "orders";

    @Autowired
    private KafkaTemplate<String, Event> kafkaTemplate;

    public void publish(Event event) {

        kafkaTemplate.send(
                TOPIC,
                event.getEventId(),
                event
        );
    }
}
