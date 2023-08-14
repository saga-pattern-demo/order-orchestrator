package com.saga.orderorchestrator.config;

import com.saga.common.dto.OrchestratorResponseDTO;
import com.saga.common.dto.PaymentResponseDTO;
import com.saga.common.dto.StockResponseDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private Map<String, Object> getCommonConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        return props;
    }

    private <T> JsonDeserializer<T> getJsonDeserializer(Class<T> targetType) {
        JsonDeserializer<T> deserializer = new JsonDeserializer<>(targetType, false);
        deserializer.addTrustedPackages("*");
        return deserializer;
    }

    private <T> ConsumerFactory<String, T> createConsumerFactory(Class<T> targetType) {
        Map<String, Object> props = getCommonConsumerProps();
        JsonDeserializer<T> deserializer = getJsonDeserializer(targetType);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> createListenerContainerFactory(Class<T> targetType) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(createConsumerFactory(targetType));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrchestratorResponseDTO>
    orchestratorListenerContainerFactory() {
        return createListenerContainerFactory(OrchestratorResponseDTO.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StockResponseDTO>
    stockListenerContainerFactory() {
        return createListenerContainerFactory(StockResponseDTO.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentResponseDTO>
    paymentListenerContainerFactory() {
        return createListenerContainerFactory(PaymentResponseDTO.class);
    }
}
