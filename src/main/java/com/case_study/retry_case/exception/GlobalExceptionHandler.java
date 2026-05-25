package com.case_study.retry_case.exception;

import com.case_study.retry_case.dto.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Global exception handler for handling custom exceptions and returning proper error responses.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {
    /**
     * Handles PayloadValidationException.
     * Returns 400 Bad Request with error details.
     *
     * @param ex The PayloadValidationException
     * @return ResponseEntity with error message
     */
    @ExceptionHandler(PayloadValidationException.class)
    public ResponseEntity<ApiResponse> handlePayloadValidationException(
            PayloadValidationException ex) {

        log.error("Payload validation failed: {}", ex.getMessage());

        ApiResponse response = ApiResponse.builder()
                .success(false)
                .message("Payload validation failed: " + ex.getMessage())
                .build();

        return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
    }

    /**
     * Generic exception handler for unexpected errors.
     * Returns 500 Internal Server Error.
     *
     * @param ex The Exception
     * @return ResponseEntity with error message
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse> handleGenericException(Exception ex) {

        log.error("Unexpected error occurred", ex);

        ApiResponse response = ApiResponse.builder()
                .success(false)
                .message("An unexpected error occurred: " + ex.getMessage())
                .build();

        return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
