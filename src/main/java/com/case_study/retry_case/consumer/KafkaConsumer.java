package com.case_study.retry_case.consumer;

import com.case_study.retry_case.dto.Event;
import com.case_study.retry_case.service.DeliveryInterface;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaConsumer {
    @Autowired
    private DeliveryInterface orderDelivery;

    @KafkaListener(topics = "orders", groupId = "order_group")
    public void consume(Event event) {

        log.info("Received event: {}" , event.getEventId());

        orderDelivery.deliverEvent(event);
    }
}
