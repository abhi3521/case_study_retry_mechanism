package com.case_study.dto;

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
public class MessagePayload {
    private String messageId;
    private String data;
    private long timestamp;
    private int retryCount;
}