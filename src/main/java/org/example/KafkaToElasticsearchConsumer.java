package org.example;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonParseException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

public class KafkaToElasticsearchConsumer {
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final KafkaProducer<String, String> dlqProducer;
    private final RestHighLevelClient elasticsearchClient;
    private static final String INDEX_PREFIX = "prd_a_";
    private static final String DEFAULT_INDEX = "prd_a_unknown";
    private static final String SOURCE_TOPIC = "my-topic";
    private static final String DLQ_TOPIC = "my-topic-dlq";
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 1000;

    public KafkaToElasticsearchConsumer() {
        this.kafkaConsumer = createKafkaConsumer();
        this.dlqProducer = createDlqProducer();
        this.elasticsearchClient = createElasticsearchClient();
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-to-elasticsearch-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  // Changed to manual commit

        return new KafkaConsumer<>(props);
    }

    private KafkaProducer<String, String> createDlqProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        return new KafkaProducer<>(props);
    }

    private RestHighLevelClient createElasticsearchClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        );
    }

    private void sendToDlq(ConsumerRecord<String, String> record, Exception error) {
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
                new ProducerRecord<>(DLQ_TOPIC, record.key(), dlqValue);

        try {
            Future<RecordMetadata> future = dlqProducer.send(dlqRecord);
            RecordMetadata metadata = future.get();  // Wait for acknowledgment
            System.out.printf("Message sent to DLQ - Topic: %s, Partition: %d, Offset: %d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (Exception e) {
            System.err.println("Failed to write to DLQ: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void indexToElasticsearchWithRetry(ConsumerRecord<String, String> record) throws IOException {
        Exception lastException = null;

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                if (attempt > 0) {
                    Thread.sleep(RETRY_BACKOFF_MS * attempt);
                }

                indexToElasticsearch(record.value());
                return;  // Success - exit the method

            } catch (Exception e) {
                lastException = e;
                System.err.printf("Elasticsearch indexing attempt %d failed: %s%n",
                        attempt + 1, e.getMessage());
            }
        }

        // If we get here, all retries failed
        if (lastException != null) {
            sendToDlq(record, lastException);
        }
    }

    private String getIndexNameFromMessage(String message) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(message);
            if (jsonNode.has("product_type")) {
                String productType = jsonNode.get("product_type").asText().toLowerCase();
                return INDEX_PREFIX + productType;
            }
        } catch (Exception e) {
            System.err.println("Error parsing message JSON: " + e.getMessage());
        }
        return DEFAULT_INDEX;
    }

    private String getIndexName(String productType) {
        LocalDate today = LocalDate.now();
        return String.format("%s%s_%s", INDEX_PREFIX, productType, today.format(DateTimeFormatter.ISO_DATE));
    }

    private String getAliasName(String productType) {
        return String.format("%s%s", INDEX_PREFIX, productType);
    }

    private void ensureIndexAndAliasExist(String productType) throws IOException {
        String indexName = getIndexName(productType);
        String aliasName = getAliasName(productType);

        // Check if the index exists
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        boolean indexExists = elasticsearchClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);

        if (!indexExists) {
            // Create the index
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName)
                    .source("{\n" +
                            "    \"aliases\": {\n" +
                            "        \"" + aliasName + "\": {}\n" +
                            "    }\n" +
                            "}", XContentType.JSON);

            elasticsearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            System.out.println("Created new index: " + indexName + " with alias: " + aliasName);
        }
    }

    private void indexToElasticsearch(String message) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode jsonNodes = mapper.readTree(message);

            if (jsonNodes.isArray()) {
                System.out.println("Processing array of " + jsonNodes.size() + " items");
                for (JsonNode node : jsonNodes) {
                    String productType = node.has("product_type") ?
                            node.get("product_type").asText().toLowerCase() : "unknown";
                    String aliasName = getAliasName(productType);

                    System.out.println("Using alias name: " + aliasName + " for item: " + node.toString());
                    ensureIndexAndAliasExist(productType);

                    IndexRequest indexRequest = new IndexRequest(aliasName, "_doc")
                            .id(UUID.randomUUID().toString())
                            .source(node.toString(), XContentType.JSON);

                    IndexResponse response = elasticsearchClient.index(indexRequest, RequestOptions.DEFAULT);
                    System.out.printf("Document indexed - ID: %s, Result: %s%n",
                            response.getId(), response.getResult().name());
                }
            } else {
                // Handle single object
                String productType = jsonNodes.has("product_type") ?
                        jsonNodes.get("product_type").asText().toLowerCase() : "unknown";
                String aliasName = getAliasName(productType);

                System.out.println("Using alias name: " + aliasName);
                ensureIndexAndAliasExist(productType);

                IndexRequest indexRequest = new IndexRequest(aliasName, "_doc")
                        .id(UUID.randomUUID().toString())
                        .source(message, XContentType.JSON);

                IndexResponse response = elasticsearchClient.index(indexRequest, RequestOptions.DEFAULT);
                System.out.printf("Document indexed - ID: %s, Result: %s%n",
                        response.getId(), response.getResult().name());
            }
        } catch (JsonParseException e) {
            System.err.println("Invalid JSON message: " + message);
            throw new IOException("Failed to parse JSON message", e);
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
                        // Commit offset only after successful processing or DLQ
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
            dlqProducer.close();
            elasticsearchClient.close();
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