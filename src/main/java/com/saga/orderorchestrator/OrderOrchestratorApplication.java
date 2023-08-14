package com.saga.orderorchestrator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class OrderOrchestratorApplication {

	public static void main(String[] args) {
		SpringApplication.run(OrderOrchestratorApplication.class, args);
	}

}
