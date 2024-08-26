package com.example.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DeadLetterConsumer {

    @KafkaListener(topics = "my-dlq")
    public void consumeFromDLQ(String message) {
        System.out.println("Received message from DLQ: " + message);
        // Handle the failed message, e.g., log, retry, or store in a database
    }
}
