package com.saga.orderorchestrator.service;

import com.saga.common.dto.OrchestratorResponseDTO;
import com.saga.common.dto.PaymentResponseDTO;
import com.saga.common.dto.StockResponseDTO;
import com.saga.common.enums.OrderStatus;
import com.saga.common.enums.PaymentStatus;
import com.saga.common.enums.StockStatus;
import com.saga.orderorchestrator.producer.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class OrderOrchestratorService {
    private final KafkaProducer kafkaProducer;
    private OrchestratorResponseDTO orchestratorResponseDTO;

    public OrderOrchestratorService(
            KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @KafkaListener(
            topics = "${topic.name.order.created}",
            groupId = "${spring.kafka.consumer.order-group-id}",
            containerFactory = "orchestratorListenerContainerFactory"
    )
    private void emitEvent(OrchestratorResponseDTO responseDTO) {
        this.orchestratorResponseDTO = responseDTO;
        kafkaProducer.sendStock(responseDTO);
    }

    @Transactional
    public void processOrder(OrchestratorResponseDTO responseDTO, OrderStatus status) {
        responseDTO.setStatus(status);
        kafkaProducer.updateOrder(responseDTO);
    }

    @KafkaListener(
            topics = "${topic.name.stock.in}",
            groupId = "${spring.kafka.consumer.stock-group-id}",
            containerFactory = "stockListenerContainerFactory"
    )
    private void getStockMessage(StockResponseDTO stockResponseDTO) {
        if (stockResponseDTO.getStatus().equals(StockStatus.AVAILABLE)) {
            kafkaProducer.sendPayment(this.orchestratorResponseDTO);
        } else {
            processOrder(this.orchestratorResponseDTO, OrderStatus.ORDER_CANCELLED);
        }
    }

    @KafkaListener(
            topics = "${topic.name.payment.in}",
            groupId = "${spring.kafka.consumer.payment-group-id}",
            containerFactory = "paymentListenerContainerFactory"
    )
    private void getPaymentMessage(PaymentResponseDTO paymentResponseDTO) {
        if (paymentResponseDTO.getStatus().equals(PaymentStatus.PAYMENT_APPROVED)) {
            processOrder(this.orchestratorResponseDTO, OrderStatus.ORDER_COMPLETED);
        } else {
            kafkaProducer.sendCancelOrder(this.orchestratorResponseDTO);
            processOrder(this.orchestratorResponseDTO, OrderStatus.ORDER_CANCELLED);
        }
    }
}
