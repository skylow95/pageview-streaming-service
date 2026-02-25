package com.pageview.pipeline.health;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

@Component
public class KafkaStreamsHealthIndicator implements HealthIndicator {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public KafkaStreamsHealthIndicator(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    @Override
    public Health health() {
        try {
            KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
            if (kafkaStreams == null) {
                return Health.down()
                        .withDetail("reason", "KafkaStreams not yet started")
                        .build();
            }
            State state = kafkaStreams.state();
            boolean isUp = state == State.RUNNING || state == State.REBALANCING;
            return isUp
                    ? Health.up().withDetail("state", state.name()).build()
                    : Health.down().withDetail("state", state.name()).build();
        } catch (Exception e) {
            return Health.down().withException(e).build();
        }
    }
}
