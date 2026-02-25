package com.pageview.pipeline.controller;

import com.pageview.pipeline.model.Pageview;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class PageviewControllerTest {

    @Test
    @DisplayName("Produce single pageview sends to Kafka with postcode as key")
    @SuppressWarnings("unchecked")
    void produceSinglePageview_SendsToKafka() {
        KafkaTemplate<String, Pageview> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(eq("pageviews"), eq("SW19"), any(Pageview.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        PageviewController controller = new PageviewController(kafkaTemplate, "pageviews");
        Pageview pageview = new Pageview(1234, "SW19", "www.website.com/index.html", 1611662684L);

        controller.produce(pageview);

        verify(kafkaTemplate).send("pageviews", "SW19", pageview);
    }

    @Test
    @DisplayName("Produce batch sends all pageviews to Kafka")
    @SuppressWarnings("unchecked")
    void produceBatch_SendsAllToKafka() {
        KafkaTemplate<String, Pageview> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(anyString(), anyString(), any(Pageview.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        PageviewController controller = new PageviewController(kafkaTemplate, "pageviews");
        List<Pageview> pageviews = List.of(
                new Pageview(1, "SW19", "www.a.com", 1611662684L),
                new Pageview(2, "EC1", "www.b.com", 1611662685L)
        );

        controller.produceBatch(pageviews);

        verify(kafkaTemplate).send(eq("pageviews"), eq("SW19"), any(Pageview.class));
        verify(kafkaTemplate).send(eq("pageviews"), eq("EC1"), any(Pageview.class));
        verify(kafkaTemplate, times(2)).send(anyString(), anyString(), any(Pageview.class));
    }
}
