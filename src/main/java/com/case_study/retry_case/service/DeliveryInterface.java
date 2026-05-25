package com.case_study.retry_case.service;

import com.case_study.retry_case.dto.Event;
import org.springframework.stereotype.Service;

@Service
public interface DeliveryInterface {
    void deliverEvent(Event event);
}
