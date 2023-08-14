package com.saga.orderorchestrator.service;

import com.saga.common.dto.*;
import com.saga.common.enums.OrderStatus;
import com.saga.common.enums.PaymentStatus;
import com.saga.common.enums.StockStatus;
import com.saga.orderorchestrator.producer.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class OrderOrchestratorService {
    private final KafkaProducer kafkaProducer;
    private OrchestratorResponseDTO orchestratorResponseDTO;
    private CompletableFuture<StockResponseDTO> stockResponseFuture;
    private CompletableFuture<PaymentResponseDTO> paymentResponseFuture;

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
        stockResponseFuture = new CompletableFuture<>();
        paymentResponseFuture = new CompletableFuture<>();
        kafkaProducer.sendStock(responseDTO);
        kafkaProducer.sendPayment(responseDTO);
        processOrder();
    }

    @Transactional
    public void processOrder() {
        CompletableFuture.allOf(stockResponseFuture, paymentResponseFuture).join();

        StockResponseDTO stockResponseDTO = stockResponseFuture.join();
        PaymentResponseDTO paymentResponseDTO = paymentResponseFuture.join();

        if (stockResponseDTO != null && stockResponseDTO.getStatus().equals(StockStatus.UNAVAILABLE) ||
                paymentResponseDTO != null && paymentResponseDTO.getStatus().equals(PaymentStatus.PAYMENT_REJECTED)) {
            orchestratorResponseDTO.setStatus(OrderStatus.ORDER_CANCELLED);
            kafkaProducer.sendCancelOrder(orchestratorResponseDTO);
        } else {
            orchestratorResponseDTO.setStatus(OrderStatus.ORDER_COMPLETED);
        }

        kafkaProducer.updateOrder(orchestratorResponseDTO);

    }

    @KafkaListener(
            topics = "${topic.name.stock.in}",
            groupId = "${spring.kafka.consumer.stock-group-id}",
            containerFactory = "stockListenerContainerFactory"
    )
    private void getStockMessage(StockResponseDTO stockResponseDTO) {
        log.info(String.valueOf(stockResponseDTO));
        stockResponseFuture.complete(stockResponseDTO);
    }

    @KafkaListener(
            topics = "${topic.name.payment.in}",
            groupId = "${spring.kafka.consumer.payment-group-id}",
            containerFactory = "paymentListenerContainerFactory"
    )
    private void getPaymentMessage(PaymentResponseDTO paymentResponseDTO) {
        log.info(String.valueOf(paymentResponseDTO));
        paymentResponseFuture.complete(paymentResponseDTO);
    }
}
