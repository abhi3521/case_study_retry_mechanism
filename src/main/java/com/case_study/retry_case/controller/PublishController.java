package com.case_study.retry_case.controller;

import com.case_study.retry_case.dto.Event;
import com.case_study.retry_case.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/publish")
public class PublishController {
    @Autowired
    private ProducerService producerService;

    @PostMapping
    public ResponseEntity<String> publish(
            @RequestBody Event event) {

        producerService.publish(event);

        return ResponseEntity.ok("Event Published");
    }
}
