package com.everflowx.esmigration.exception;

/**
 * 数据迁移过程异常
 * 
 * @author everflowx
 */
public class DataMigrationException extends EsMigrationException {
    
    private long processedCount;
    private long failedCount;
    private String batchId;
    
    public DataMigrationException(String message, String indexName) {
        super("MIGRATION_ERROR", message, indexName);
    }
    
    public DataMigrationException(String message, String indexName, Throwable cause) {
        super("MIGRATION_ERROR", message, indexName, cause);
    }
    
    public DataMigrationException(String message, String indexName, long processedCount, long failedCount) {
        super("MIGRATION_ERROR", message, indexName);
        this.processedCount = processedCount;
        this.failedCount = failedCount;
    }
    
    public DataMigrationException(String message, String indexName, String batchId, Throwable cause) {
        super("MIGRATION_ERROR", message, indexName, cause);
        this.batchId = batchId;
    }
    
    public long getProcessedCount() {
        return processedCount;
    }
    
    public long getFailedCount() {
        return failedCount;
    }
    
    public String getBatchId() {
        return batchId;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DataMigrationException{");
        sb.append("indexName='").append(getIndexName()).append("'");
        if (processedCount > 0) {
            sb.append(", processedCount=").append(processedCount);
        }
        if (failedCount > 0) {
            sb.append(", failedCount=").append(failedCount);
        }
        if (batchId != null) {
            sb.append(", batchId='").append(batchId).append("'");
        }
        sb.append(", message='").append(getMessage()).append("'");
        sb.append("}");
        return sb.toString();
    }
    
    public static DataMigrationException batchProcessingFailed(String indexName, String batchId, int batchSize, Throwable cause) {
        return new DataMigrationException(
            String.format("批次数据处理失败，批次ID: %s, 批次大小: %d", batchId, batchSize),
            indexName, batchId, cause
        );
    }
    
    public static DataMigrationException documentTransformFailed(String indexName, String documentId, Throwable cause) {
        return new DataMigrationException(
            String.format("文档转换失败，文档ID: %s", documentId),
            indexName, cause
        );
    }
    
    public static DataMigrationException bulkInsertFailed(String indexName, int documentCount, Throwable cause) {
        return new DataMigrationException(
            String.format("批量插入失败，文档数量: %d", documentCount),
            indexName, cause
        );
    }
    
    public static DataMigrationException scrollSearchFailed(String indexName, String scrollId, Throwable cause) {
        return new DataMigrationException(
            String.format("滚动查询失败，ScrollID: %s", scrollId != null ? scrollId.substring(0, Math.min(20, scrollId.length())) + "..." : "null"),
            indexName, cause
        );
    }
    
    public static DataMigrationException bulkOperationFailed(String indexName, String batchId, int failedCount, Throwable cause) {
        return new DataMigrationException(
            String.format("批量操作失败，失败数量: %d", failedCount),
            indexName, batchId, cause
        );
    }
}