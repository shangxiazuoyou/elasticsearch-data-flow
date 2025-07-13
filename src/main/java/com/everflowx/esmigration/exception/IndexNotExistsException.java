package com.everflowx.esmigration.exception;

/**
 * 索引不存在异常
 * 
 * @author everflowx
 */
public class IndexNotExistsException extends EsMigrationException {
    
    public IndexNotExistsException(String indexName) {
        super("INDEX_NOT_EXISTS", "索引不存在: " + indexName, indexName);
    }
    
    public IndexNotExistsException(String indexName, String esType) {
        super("INDEX_NOT_EXISTS", 
              String.format("索引不存在: %s (ES类型: %s)", indexName, esType), 
              indexName);
    }
}