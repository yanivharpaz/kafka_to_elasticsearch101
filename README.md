# Kafka to Elasticsearch Consumer

This application consumes messages from a Kafka topic and indexes them into Elasticsearch. It includes features like Dead Letter Queue (DLQ) handling, dynamic index creation with aliases based on product type, and retry mechanisms.

## Features

- Consumes JSON messages from Kafka
- Dynamic index creation with aliases based on product_type field
- Dead Letter Queue (DLQ) for failed messages
- Retry mechanism for failed Elasticsearch operations
- Manual offset commit for better control
- Detailed logging and error handling

## Prerequisites

- Java 8 or higher
- Gradle
- Apache Kafka (running on localhost:9092)
- Elasticsearch (running on localhost:9200)

## Setup

1. Clone the repository:
```bash
git clone [repository-url]
cd kafka-to-elasticsearch-consumer
```

2. Build the project:
```bash
./gradlew build
```

3. Run the application:
```bash
./gradlew run
```

## Configuration

### Kafka Configuration

The following Kafka configurations can be modified in the code:

```java
// Consumer Configuration
bootstrap.servers=localhost:9092
group.id=kafka-to-elasticsearch-consumer
auto.offset.reset=earliest
enable.auto.commit=false

// DLQ Producer Configuration
bootstrap.servers=localhost:9092
acks=all
retries=3
```

### Elasticsearch Configuration

The following Elasticsearch configurations can be modified:

```java
// Client Configuration
host=localhost
port=9200
scheme=http

// Index/Alias Naming
index.prefix=prd_a_
```

## Message Format

The application expects JSON messages in the following format:

```json
{
    "item_id": "12345",
    "category_id": "67890",
    "name": "Sample Item",
    "product_type": "widget",
    "dept": "Hardware"
}
```

Or an array of such objects:

```json
[
    {
        "item_id": "12345",
        "product_type": "widget",
        ...
    },
    {
        "item_id": "67890",
        "product_type": "gadget",
        ...
    }
]
```

## Index and Alias Naming

- Indices are created with names in the format: `prd_a_[product_type]_[date]`
  Example: `prd_a_widget_2024-12-21`
- Aliases are created in the format: `prd_a_[product_type]`
  Example: `prd_a_widget`

## Dead Letter Queue

Failed messages are sent to a DLQ topic (`my-topic-dlq`) with the following information:
- Original message
- Original topic/partition/offset
- Error details
- Timestamp
- Stack trace

## Error Handling

- Retry mechanism with configurable attempts (default: 3)
- Exponential backoff between retries
- DLQ for messages that fail after all retries
- Detailed error logging

## Development

### Project Structure

```
src/
└── main/
    └── java/
        └── org/
            └── example/
                └── KafkaToElasticsearchConsumer.java
```

### Key Components

1. `KafkaToElasticsearchConsumer`: Main class that handles:
   - Kafka message consumption
   - Elasticsearch indexing
   - DLQ handling
   - Error retry logic

2. Key Methods:
   - `indexToElasticsearch`: Handles message indexing
   - `ensureIndexAndAliasExist`: Manages index and alias creation
   - `sendToDlq`: Handles failed messages
   - `indexToElasticsearchWithRetry`: Implements retry logic

## Monitoring

The application provides detailed logging for:
- Message processing
- Index/alias creation
- Elasticsearch operations
- Error conditions
- DLQ operations

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Add your license information here]