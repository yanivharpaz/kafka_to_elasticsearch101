package org.example;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonParseException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
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
import java.util.concurrent.ConcurrentHashMap;

public class KafkaToElasticsearchConsumer_full {
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final KafkaProducer<String, String> dlqProducer;
    private final RestHighLevelClient elasticsearchClient;
    private final Map<String, String> aliasCache;  // Cache to store alias -> index mappings
    private final Map<String, Integer> indexCounters;  // Cache to store product type -> current index number
    private static final String INDEX_PREFIX = "prd_a_";
    private static final String DEFAULT_INDEX = "prd_a_unknown";
    private static final String SOURCE_TOPIC = "my-topic";
    private static final String DLQ_TOPIC = "my-topic-dlq";
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 1000;

    public KafkaToElasticsearchConsumer_full() {
        this.kafkaConsumer = createKafkaConsumer();
        this.dlqProducer = createDlqProducer();
        this.elasticsearchClient = createElasticsearchClient();
        this.aliasCache = new ConcurrentHashMap<>();
        this.indexCounters = new ConcurrentHashMap<>();
        initializeAliasCache();
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-to-elasticsearch-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

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

    private void initializeAliasCache() {
        try {
            // Get all indices using low-level client
            Response response = elasticsearchClient.getLowLevelClient()
                    .performRequest(new Request("GET", "/_alias"));

            // Parse the response
            String responseBody = EntityUtils.toString(response.getEntity());
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> aliasMap = mapper.readValue(responseBody, Map.class);

            // Process each index and its aliases
            for (Map.Entry<String, Object> entry : aliasMap.entrySet()) {
                String indexName = entry.getKey();
                // Update index counters based on existing indices
                if (indexName.startsWith(INDEX_PREFIX)) {
                    try {
                        String[] parts = indexName.split("_");
                        if (parts.length >= 4) {  // prefix_producttype_number
                            String productType = parts[parts.length - 2];
                            int indexNumber = Integer.parseInt(parts[parts.length - 1]);
                            indexCounters.merge(productType, indexNumber, Integer::max);
                        }
                    } catch (NumberFormatException e) {
                        // Skip if the index name doesn't match our expected format
                        continue;
                    }
                }

                @SuppressWarnings("unchecked")
                Map<String, Object> aliasInfo = (Map<String, Object>) entry.getValue();

                if (aliasInfo.containsKey("aliases")) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> aliases = (Map<String, Object>) aliasInfo.get("aliases");
                    for (String aliasName : aliases.keySet()) {
                        if (aliasName.startsWith(INDEX_PREFIX)) {
                            aliasCache.put(aliasName, indexName);
                            System.out.println("Cached alias mapping: " + aliasName + " -> " + indexName);
                        }
                    }
                }
            }
            System.out.println("Initialized alias cache with " + aliasCache.size() + " entries");
            System.out.println("Initialized index counters: " + indexCounters);
        } catch (Exception e) {
            System.err.println("Error initializing alias cache: " + e.getMessage());
            e.printStackTrace();
        }
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
            RecordMetadata metadata = future.get();
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
                return;

            } catch (Exception e) {
                lastException = e;
                System.err.printf("Elasticsearch indexing attempt %d failed: %s%n",
                        attempt + 1, e.getMessage());
            }
        }

        if (lastException != null) {
            sendToDlq(record, lastException);
        }
    }

    private synchronized int getNextIndexNumber(String productType) {
        int currentNumber = indexCounters.getOrDefault(productType, 0);
        int nextNumber = currentNumber + 1;
        indexCounters.put(productType, nextNumber);
        return nextNumber;
    }

    private String getIndexName(String productType) {
        return String.format("%s%s_%05d", INDEX_PREFIX, productType, getNextIndexNumber(productType));
    }

    private String getAliasName(String productType) {
        return String.format("%s%s", INDEX_PREFIX, productType);
    }

    private void ensureIndexAndAliasExist(String productType) throws IOException {
        String indexName = getIndexName(productType);
        String aliasName = getAliasName(productType);

        System.out.println("Checking index and alias existence for product type: " + productType);
        System.out.println("Index name: " + indexName + ", Alias name: " + aliasName);

        try {
            // First check the cache
            String existingIndex = aliasCache.get(aliasName);
            if (existingIndex != null) {
                System.out.println("Found existing alias in cache: " + aliasName + " -> " + existingIndex);

                // Verify that the cached index actually exists
                GetIndexRequest verifyRequest = new GetIndexRequest(existingIndex);
                boolean indexExists = elasticsearchClient.indices().exists(verifyRequest, RequestOptions.DEFAULT);
                if (!indexExists) {
                    System.out.println("Warning: Cached index " + existingIndex + " no longer exists. Will create new index and alias.");
                    aliasCache.remove(aliasName);
                } else {
                    System.out.println("Verified cached index exists. Using existing index and alias.");
                    return;
                }
            }

            // Check if the new index exists
            GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
            boolean indexExists = elasticsearchClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);

            if (!indexExists) {
                try {
                    // Create the index
                    System.out.println("Creating new index: " + indexName);
                    CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                    createIndexRequest.source("{\n" +
                            "    \"settings\": {\n" +
                            "        \"number_of_shards\": 3,\n" +
                            "        \"number_of_replicas\": 1\n" +
                            "    }\n" +
                            "}", XContentType.JSON);

                    elasticsearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                    System.out.println("Successfully created index: " + indexName);

                    try {
                        // Create the alias using low-level client
                        System.out.println("Creating alias: " + aliasName + " -> " + indexName);
                        String aliasPayload = String.format("""
                            {
                                "actions": [
                                    {
                                        "add": {
                                            "index": "%s",
                                            "alias": "%s",
                                            "is_write_index": true
                                        }
                                    }
                                ]
                            }""", indexName, aliasName);

                        Request aliasRequest = new Request("POST", "/_aliases");
                        aliasRequest.setJsonEntity(aliasPayload);

                        Response aliasResponse = elasticsearchClient.getLowLevelClient().performRequest(aliasRequest);
                        int statusCode = aliasResponse.getStatusLine().getStatusCode();

                        if (statusCode >= 200 && statusCode < 300) {
                            System.out.println("Successfully created alias: " + aliasName);
                            aliasCache.put(aliasName, indexName);
                            System.out.println("Updated alias cache: " + aliasName + " -> " + indexName);
                        } else {
                            throw new IOException("Failed to create alias. Status code: " + statusCode);
                        }

                    } catch (Exception e) {
                        System.err.println("Error creating alias: " + e.getMessage());
                        // Try to clean up the index if alias creation failed
                        try {
                            System.out.println("Attempting to clean up index after alias creation failure");
                            elasticsearchClient.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
                            System.out.println("Successfully cleaned up index: " + indexName);
                        } catch (Exception cleanupError) {
                            System.err.println("Failed to clean up index after alias creation failure: " + cleanupError.getMessage());
                        }
                        throw new IOException("Failed to create alias", e);
                    }
                } catch (Exception e) {
                    System.err.println("Error creating index: " + e.getMessage());
                    throw new IOException("Failed to create index and alias", e);
                }
            } else {
                System.out.println("Index " + indexName + " already exists. Verifying alias.");
                // Verify if the alias exists and create if it doesn't
                try {
                    Request checkAliasRequest = new Request("GET", "/_alias/" + aliasName);
                    Response checkAliasResponse = elasticsearchClient.getLowLevelClient().performRequest(checkAliasRequest);

                    if (checkAliasResponse.getStatusLine().getStatusCode() == 404) {
                        System.out.println("Alias doesn't exist. Creating alias: " + aliasName);
                        // Create the alias
                        String aliasPayload = String.format("""
                            {
                                "actions": [
                                    {
                                        "add": {
                                            "index": "%s",
                                            "alias": "%s",
                                            "is_write_index": true
                                        }
                                    }
                                ]
                            }""", indexName, aliasName);

                        Request aliasRequest = new Request("POST", "/_aliases");
                        aliasRequest.setJsonEntity(aliasPayload);
                        elasticsearchClient.getLowLevelClient().performRequest(aliasRequest);
                        System.out.println("Successfully created alias: " + aliasName);
                    } else {
                        System.out.println("Alias " + aliasName + " already exists");
                    }

                    aliasCache.put(aliasName, indexName);
                    System.out.println("Updated alias cache: " + aliasName + " -> " + indexName);
                } catch (Exception e) {
                    System.err.println("Error verifying/creating alias: " + e.getMessage());
                    throw new IOException("Failed to verify/create alias", e);
                }
            }
        } catch (Exception e) {
            System.err.println("Critical error in ensureIndexAndAliasExist: " + e.getMessage());
            e.printStackTrace();
            throw new IOException("Failed to ensure index and alias existence", e);
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
        KafkaToElasticsearchConsumer_full consumer = new KafkaToElasticsearchConsumer_full();
        consumer.start();
    }
}