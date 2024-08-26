//package com.example.kafka.producer;
//////
//import org.springframework.beans.factory.annotation.Autowired;
//////import org.springframework.kafka.core.KafkaTemplate;
//////import org.springframework.stereotype.Component;
//////
//////@Component
//////public class MessageProducer {
//////
//////    @Autowired
//////    private KafkaTemplate<String, String> kafkaTemplate;
//////
//////    public void sendMessage(String topic, String message) {
//////        kafkaTemplate.send(topic, message);
//////    }
//////
//////}
////import org.springframework.kafka.core.KafkaTemplate;
////import org.springframework.kafka.support.KafkaHeaders;
////import org.springframework.kafka.support.SendResult;
////import org.springframework.messaging.support.MessageBuilder;
////import org.springframework.stereotype.Component;
////
////import java.util.concurrent.CompletableFuture;
////import java.util.concurrent.TimeUnit;
////
////@Component
////public class MessageProducer {
////
////    private static final int MAX_RETRIES = 3;
////    private static final long INITIAL_BACKOFF = 1000L; // 1 second
////    private static final String DEAD_LETTER_TOPIC = "my-dlq";
////
////    @Autowired
////    private KafkaTemplate<String, String> kafkaTemplate;
////
////    public void sendMessage(String topic, String message) {
////        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);
////        future.whenComplete((result, ex) -> {
////            if (ex != null) {
////                int retryCount = 0;
////                long backoff = INITIAL_BACKOFF;
////                while (retryCount < MAX_RETRIES) {
////                    try {
////                        Thread.sleep(backoff);
////                        kafkaTemplate.send(topic, message).get();
////                        break;
////                    } catch (Exception e) {
////                        // Handle exception, log, or send to dead-letter topic
////                        retryCount++;
////                        backoff *= 2; // Exponential backoff
////                    }
////                }
////                if (retryCount == MAX_RETRIES) {
////                    // Send message to dead-letter topic
////                    kafkaTemplate.send(DEAD_LETTER_TOPIC, message);
////                }
////            }
////        });
////    }
////}
////
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.support.KafkaHeaders;
//import org.springframework.kafka.support.SendResult;
//import org.springframework.messaging.support.MessageBuilder;
//import org.springframework.stereotype.Component;
//
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.TimeUnit;
//
//@Component
//public class MessageProducer {
//
//    private static final int MAX_RETRIES = 3;
//    private static final long INITIAL_BACKOFF = 1000L; // 1 second
//    private static final String DEAD_LETTER_TOPIC = "my-dlq";
//
//    @Autowired
//    private KafkaTemplate<String, String> kafkaTemplate;
//
//    public void sendMessage(String topic, String message) {
//        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);
//
//        future.whenComplete((result, ex) -> {
//            if (ex != null)
//            {
//                int retryCount = 0;
//                long backoff = INITIAL_BACKOFF;
//                while (retryCount < MAX_RETRIES) {
//                    try {
//                        Thread.sleep(backoff);
//                        kafkaTemplate.send(topic, message).get();
//                        break; // Exit retry loop on successful retry
//                    } catch (Exception e) {
//                        // Handle exception, log, or send to dead-letter topic
//                        retryCount++;
//                        backoff *= 2; // Exponential backoff
//                    }
//                }
//                if (retryCount == MAX_RETRIES) {
//                    // Send message to dead-letter topic
//                    kafkaTemplate.send(DEAD_LETTER_TOPIC, message);
//                }
//            } else {
//                // ... normal processing ...
//                if (message.contains("error")) {
//                    throw new RuntimeException("Simulated error for testing");
//                }
//            }
//        });
//    }
//}
//


package com.example.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class MessageProducer {

    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF = 1000L; // 1 second
    private static final String DEAD_LETTER_TOPIC = "my-dlq";
    private static final long SAME_MESSAGE_DELAY = 1000L; // 1 seconds

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private String lastMessage = null;
    private long lastMessageTime = 0;
    private long delay =1;
    public void sendMessage(String topic, String message) {
        // Check if the same message is sent continuously and add delay

        if (message.equals(lastMessage)) {
            long currentTime = System.currentTimeMillis();
            System.out.println("current time is  "+ currentTime +", last message time is "+lastMessageTime);
             delay *=  SAME_MESSAGE_DELAY ;
            System.out.println(delay);
            if (delay > 0) {
                try {
                    System.out.println("Delaying message by " + delay + " ms");
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        lastMessage = message;
        lastMessageTime = System.currentTimeMillis();


        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                int retryCount = 0;
                long backoff = INITIAL_BACKOFF;
                while (retryCount < MAX_RETRIES) {
                    try {
                        System.out.println("Retrying message, attempt " + (retryCount + 1) + ", backoff " + backoff + " ms");
                        Thread.sleep(backoff);
                        kafkaTemplate.send(topic, message).get();
                        System.out.println("Message sent successfully after retry");
                        break; // Exit retry loop on successful retry
                    } catch (Exception e) {
                        System.out.println("Retry failed, attempt " + (retryCount + 1));
                        retryCount++;
                        backoff *= 2; // Exponential backoff
                    }
                }
                if (retryCount == MAX_RETRIES) {
                    System.out.println("Sending message to dead-letter topic");
                    kafkaTemplate.send(DEAD_LETTER_TOPIC, message);
                }
            } else {
                // Normal processing
                System.out.println("Message sent successfully: " + message);
                if (message.contains("error")) {
                    throw new RuntimeException("Simulated error for testing");
                }
            }
        });
    }
}
