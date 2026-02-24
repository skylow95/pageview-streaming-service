package com.pageview.pipeline.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pageview.pipeline.model.Pageview;
import com.pageview.pipeline.model.PageviewAggregate;
import io.micrometer.core.instrument.Metrics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaFactory {

    private final String bootstrapServers;

    public KafkaFactory(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @Bean
    public ProducerFactory<String, Pageview> pageviewProducerFactory(
            @Qualifier("jsonObjectMapper") ObjectMapper jsonObjectMapper) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config, new StringSerializer(),
                new JsonSerializer<>(jsonObjectMapper));
    }

    @Bean
    public KafkaTemplate<String, Pageview> pageviewKafkaTemplate(ProducerFactory<String, Pageview> factory) {
        return new KafkaTemplate<>(factory);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Pageview> pageviewListenerFactory(
            @Qualifier("jsonObjectMapper") ObjectMapper jsonObjectMapper) {
        ConcurrentKafkaListenerContainerFactory<String, Pageview> factory = new ConcurrentKafkaListenerContainerFactory<>();
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Pageview.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        DefaultKafkaConsumerFactory<String, Pageview> cf = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new JsonDeserializer<>(Pageview.class, jsonObjectMapper));
        cf.addListener(new MicrometerConsumerListener<>(Metrics.globalRegistry, Collections.emptyList()));
        factory.setConsumerFactory(cf);

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PageviewAggregate> aggregateListenerFactory(
            @Qualifier("jsonObjectMapper") ObjectMapper jsonObjectMapper) {
        ConcurrentKafkaListenerContainerFactory<String, PageviewAggregate> factory = new ConcurrentKafkaListenerContainerFactory<>();
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, PageviewAggregate.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        DefaultKafkaConsumerFactory<String, PageviewAggregate> cf = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new JsonDeserializer<>(PageviewAggregate.class, jsonObjectMapper));
        cf.addListener(new MicrometerConsumerListener<>(Metrics.globalRegistry, Collections.emptyList()));
        factory.setConsumerFactory(cf);

        return factory;
    }
}
