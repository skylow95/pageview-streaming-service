package com.pageview.pipeline.factory;

import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@EnableKafkaStreams
public class KafkaStreamsFactory {

    private final String bootstrapServers;
    private final String applicationId;
    private final int streamThreads;

    public KafkaStreamsFactory(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                               @Value("${spring.application.name:pageview-pipeline}") String applicationId,
                               @Value("${pipeline.stream.threads:2}") int streamThreads) {
        this.bootstrapServers = bootstrapServers;
        this.applicationId = applicationId;
        this.streamThreads = streamThreads;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfig() {
        return new KafkaStreamsConfiguration(Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, applicationId + "-streams",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads
        ));
    }
}
