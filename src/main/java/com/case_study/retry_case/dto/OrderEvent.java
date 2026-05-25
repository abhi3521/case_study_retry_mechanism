package com.case_study.retry_case.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEvent {
    private String eventId;
    private String orderId;
    private String customerId;
    private Double amount;
    private String status;
}
