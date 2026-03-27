package com.mna.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@EnableRabbit
public class RabbitStreamsConfiguration {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    private final Map<String, Test> aggregationStore = new ConcurrentHashMap<>();

    @Bean
    public Queue inputQueue() {
        return new Queue("streams-json-input");
    }

    @Bean
    public Queue outputQueue() {
        return new Queue("streams-json-output");
    }

    @RabbitListener(queuesToDeclare = @org.springframework.amqp.rabbit.annotation.Queue(name = "streams-json-input"))
    public void handleMessage(Test message) {
        // Aggregate words by key
        aggregationStore.compute(message.getKey(), (key, existing) -> {
            if (existing == null) {
                Test newTest = new Test();
                newTest.setKey(key);
                newTest.getWords().addAll(message.getWords());
                return newTest;
            } else {
                existing.getWords().addAll(message.getWords());
                return existing;
            }
        });

        // Send the aggregated result to the output queue
        Test aggregated = aggregationStore.get(message.getKey());
        rabbitTemplate.convertAndSend("streams-json-output", aggregated);
    }

    @Data
    public static class Test {

        private String key;
        private List<String> words;

        public Test() {
            this.words = new ArrayList<>();
        }
    }
}