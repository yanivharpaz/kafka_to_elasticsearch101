package org.example.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class DlqService {
    private final KafkaProducer<String, String> producer;
    private final String dlqTopic;

    public DlqService(KafkaProducer<String, String> producer, String dlqTopic) {
        this.producer = producer;
        this.dlqTopic = dlqTopic;
    }

    public void sendToDlq(ConsumerRecord<String, String> record, Exception error) {
        Map<String, Object> dlqMessage = new HashMap<>();
        dlqMessage.put("original_message", record.value());
        dlqMessage.put("original_topic", record.topic());
        dlqMessage.put("original_partition", record.partition());
        dlqMessage.put("original_offset", record.offset());
        dlqMessage.put("error_message", error.getMessage());
        dlqMessage.put("error_timestamp", System.currentTimeMillis());
        dlqMessage.put("error_stacktrace", error.toString());

        String dlqValue = dlqMessage.toString();
        ProducerRecord<String, String> dlqRecord = 
            new ProducerRecord<>(dlqTopic, record.key(), dlqValue);

        try {
            Future<RecordMetadata> future = producer.send(dlqRecord);
            RecordMetadata metadata = future.get();
            System.out.printf("Message sent to DLQ - Topic: %s, Partition: %d, Offset: %d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (Exception e) {
            System.err.println("Failed to write to DLQ: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
