package com.kafkaworkshop.urlshortener.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaOperations;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public class IdentifierFactory {

    private final KafkaOperations<String, String> kafkaOperations;
    private final String instanceIdsTopic;
    private final AtomicLong atomicLong;

    private String instanceId;

    public IdentifierFactory(KafkaOperations<String, String> kafkaOperations, String instanceIdsTopic) {
        this.kafkaOperations = kafkaOperations;
        this.instanceIdsTopic = instanceIdsTopic;
        this.atomicLong = new AtomicLong();
    }

    public String createShortIdentifier() throws ExecutionException, InterruptedException {
        if (instanceId == null) {
            RecordMetadata recordMetadata = kafkaOperations.send(instanceIdsTopic, " ").get().getRecordMetadata();
            instanceId = String.format("p%so%s", recordMetadata.partition(), recordMetadata.offset());
        }
        return String.format("%s-%d", instanceId, atomicLong.incrementAndGet());
    }
}
