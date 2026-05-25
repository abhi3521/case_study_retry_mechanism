package com.case_study.retry_case.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO representing the message payload passed from queue to microservice.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Event {
    private String eventId;
    private String orderId;
    private String payload;
    //private Status eventStatus;
}