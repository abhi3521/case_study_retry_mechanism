package com.case_study.retry_case.exception;

public class PayloadValidationException extends RuntimeException {
    public PayloadValidationException(String message) {
        super(message);
    }

    public PayloadValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
