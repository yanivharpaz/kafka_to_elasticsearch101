package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.example.config.ElasticsearchConfig;
import org.example.config.KafkaConfig;
import org.example.service.DlqService;
import org.example.service.ElasticsearchService;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

public class KafkaToElasticsearchConsumer {
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final ElasticsearchService elasticsearchService;
    private final DlqService dlqService;
    
    private static final String SOURCE_TOPIC = "my-topic";
    private static final String DLQ_TOPIC = "my-topic-dlq";
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 1000;

    public KafkaToElasticsearchConsumer() {
        this.kafkaConsumer = KafkaConfig.createConsumer("kafka-to-elasticsearch-consumer");
        this.elasticsearchService = new ElasticsearchService(ElasticsearchConfig.createClient());
        this.dlqService = new DlqService(KafkaConfig.createProducer(), DLQ_TOPIC);
    }

    private void indexToElasticsearchWithRetry(ConsumerRecord<String, String> record) throws IOException {
        Exception lastException = null;

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                if (attempt > 0) {
                    Thread.sleep(RETRY_BACKOFF_MS * attempt);
                }
                elasticsearchService.indexDocument(record.value());
                return;
            } catch (Exception e) {
                lastException = e;
                System.err.printf("Elasticsearch indexing attempt %d failed: %s%n",
                        attempt + 1, e.getMessage());
            }
        }

        if (lastException != null) {
            dlqService.sendToDlq(record, lastException);
        }
    }

    public void start() {
        try {
            kafkaConsumer.subscribe(Collections.singletonList(SOURCE_TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Processing message - Topic: %s, Partition: %d, Offset: %d%n",
                            record.topic(), record.partition(), record.offset());

                    try {
                        indexToElasticsearchWithRetry(record);
                        kafkaConsumer.commitSync(Collections.singletonMap(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)
                        ));
                    } catch (Exception e) {
                        System.err.println("Failed to process message: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            cleanup();
        }
    }

    private void cleanup() {
        try {
            kafkaConsumer.close();
            elasticsearchService.close();
        } catch (IOException e) {
            System.err.println("Error during cleanup: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        KafkaToElasticsearchConsumer consumer = new KafkaToElasticsearchConsumer();
        consumer.start();
    }
}