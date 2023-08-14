package com.saga.orderorchestrator.producer;

import com.saga.common.dto.OrchestratorResponseDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaProducer {
    private final KafkaTemplate<String, OrchestratorResponseDTO> kafkaTemplate;
    private final String orderUpdatedTopic;
    private final String stockOutTopic;
    private final String cancelOrderTopic;
    private final String paymentOutTopic;

    public KafkaProducer(
            KafkaTemplate<String, OrchestratorResponseDTO> kafkaTemplate,
            @Value("${topic.name.order.updated}") String orderUpdatedTopic,
            @Value("${topic.name.stock.out}") String stockOutTopic,
            @Value("${topic.name.payment.out}") String paymentOutTopic,
            @Value("${topic.name.stock.cancel}") String cancelOrderTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderUpdatedTopic = orderUpdatedTopic;
        this.stockOutTopic = stockOutTopic;
        this.paymentOutTopic = paymentOutTopic;
        this.cancelOrderTopic = cancelOrderTopic;
    }

    public void updateOrder(OrchestratorResponseDTO payload) {
        Message<OrchestratorResponseDTO> message = MessageBuilder
                .withPayload(payload)
                .setHeader(KafkaHeaders.TOPIC, orderUpdatedTopic)
                .build();
        kafkaTemplate.send(message);
    }

    public void sendStock(OrchestratorResponseDTO payload) {
        log.info("sending to stock topic={}, payload={}", stockOutTopic, payload);
        kafkaTemplate.send(stockOutTopic, payload);
    }

    public void sendCancelOrder(OrchestratorResponseDTO payload) {
        log.info("sending to stock cancel topic={}, payload={}", cancelOrderTopic, payload);
        kafkaTemplate.send(cancelOrderTopic, payload);
    }

    public void sendPayment(OrchestratorResponseDTO payload) {
        log.info("sending to payment topic={}, payload={}", paymentOutTopic, payload);
        kafkaTemplate.send(paymentOutTopic, payload);
    }

}
