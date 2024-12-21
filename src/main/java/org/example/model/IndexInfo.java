package org.example.model;

public class IndexInfo {
    private final String indexName;
    private final String aliasName;

    public IndexInfo(String indexName, String aliasName) {
        this.indexName = indexName;
        this.aliasName = aliasName;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getAliasName() {
        return aliasName;
    }
}
