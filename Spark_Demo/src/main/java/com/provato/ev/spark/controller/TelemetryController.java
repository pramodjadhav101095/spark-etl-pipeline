package com.provato.ev.spark.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.provato.ev.spark.config.service.TelemetryService;

@RestController
public class TelemetryController {

    private final TelemetryService service;

    public TelemetryController(TelemetryService service) {
        this.service = service;
    }

   
    @GetMapping("/run-job")
    public List<Map<String, Object>> runJob() {
        return service.processJson();   // updated to use JSON method
    }
}