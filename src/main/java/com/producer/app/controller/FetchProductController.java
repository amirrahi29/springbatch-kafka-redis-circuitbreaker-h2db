package com.producer.app.controller;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;

@RestController
public class FetchProductController {

	private static final Logger log = LoggerFactory.getLogger(FetchProductController.class);

	@GetMapping("/fetch-products")
	@CircuitBreaker(name = "fetchProducts", fallbackMethod = "fetchProductsFallback")
	public void fetchProducts() {
		CompletableFuture.runAsync(() -> {
			Object products = new RestTemplate().getForObject("http://localhost:8081/products", Object.class);
			log.info("fetched products {}", products);
		}).join();
	}

	public void fetchProductsFallback(Throwable throwable) {
		log.warn("circuit is open", throwable);
	}

}
