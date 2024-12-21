package org.example.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticsearchConfig {
    private static final String HOST = "localhost";
    private static final int PORT = 9200;
    private static final String SCHEME = "http";

    public static RestHighLevelClient createClient() {
        return new RestHighLevelClient(
            RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME)
            )
        );
    }
}
