package com.pageview.pipeline.controller;

import com.pageview.pipeline.model.Pageview;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/pageviews")
public class PageviewController {

    private final KafkaTemplate<String, Pageview> kafkaTemplate;
    private final String pageviewsTopic;

    public PageviewController(KafkaTemplate<String, Pageview> kafkaTemplate,
                              @Value("${pipeline.pageviews-topic:pageviews}") String pageviewsTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.pageviewsTopic = pageviewsTopic;
    }

    @PostMapping(consumes = "application/json", produces = "application/json")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void produce(@Valid @RequestBody Pageview pageview) {
        kafkaTemplate.send(pageviewsTopic, pageview.postcode(), pageview);
    }

    @PostMapping(value = "/batch", consumes = "application/json", produces = "application/json")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void produceBatch(@Valid @RequestBody List<Pageview> pageviews) {
        pageviews.forEach(pv -> kafkaTemplate.send(pageviewsTopic, pv.postcode(), pv));
    }
}
