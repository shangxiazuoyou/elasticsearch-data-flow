package com.everflowx.esmigration.checkpoint;

import lombok.Data;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 字段级别断点信息
 * 支持更细粒度的断点续传，可以精确到单个文档的字段级别
 * 
 * @author everflowx
 */
@Data
public class FieldLevelCheckpoint {
    
    /**
     * 断点ID - 唯一标识
     */
    private String checkpointId;
    
    /**
     * 父任务ID
     */
    private String taskId;
    
    /**
     * 断点类型
     */
    private CheckpointType type;
    
    /**
     * 文档级别断点信息
     */
    private DocumentCheckpoint documentCheckpoint;
    
    /**
     * 字段级别断点信息
     */
    private Map<String, FieldCheckpoint> fieldCheckpoints;
    
    /**
     * 批次级别断点信息
     */
    private BatchCheckpoint batchCheckpoint;
    
    /**
     * 分片级别断点信息
     */
    private ShardCheckpoint shardCheckpoint;
    
    /**
     * 时间范围断点信息（用于基于时间的分页迁移）
     */
    private TimeRangeCheckpoint timeRangeCheckpoint;
    
    /**
     * 创建时间
     */
    private Date createTime;
    
    /**
     * 最后更新时间
     */
    private Date lastUpdateTime;
    
    /**
     * 断点状态
     */
    private CheckpointStatus status;
    
    /**
     * 错误信息
     */
    private String errorMessage;
    
    /**
     * 重试次数
     */
    private int retryCount;
    
    /**
     * 最大重试次数
     */
    private int maxRetries = 3;
    
    /**
     * 断点类型枚举
     */
    public enum CheckpointType {
        DOCUMENT_LEVEL,    // 文档级别
        FIELD_LEVEL,       // 字段级别  
        BATCH_LEVEL,       // 批次级别
        SHARD_LEVEL,       // 分片级别
        TIME_RANGE_LEVEL   // 时间范围级别
    }
    
    /**
     * 断点状态枚举
     */
    public enum CheckpointStatus {
        PENDING,     // 待处理
        PROCESSING,  // 处理中
        COMPLETED,   // 已完成
        FAILED,      // 失败
        SUSPENDED    // 暂停
    }
    
    /**
     * 文档级别断点
     */
    @Data
    public static class DocumentCheckpoint {
        private String lastProcessedDocId;
        private long lastProcessedTimestamp;
        private String lastScrollId;
        private Object[] lastSearchAfter;  // search_after 参数
        private long processedDocCount;
        private Set<String> failedDocIds;  // 失败的文档ID集合
        private Set<String> partiallyProcessedDocIds;  // 部分处理的文档ID
    }
    
    /**
     * 字段级别断点
     */
    @Data
    public static class FieldCheckpoint {
        private String fieldName;
        private String documentId;
        private Object originalValue;
        private Object transformedValue;
        private FieldProcessStatus status;
        private String errorMessage;
        private int retryCount;
        
        public enum FieldProcessStatus {
            PENDING,        // 待处理
            TRANSFORMING,   // 转换中
            TRANSFORMED,    // 已转换
            INDEXING,       // 索引中
            INDEXED,        // 已索引
            FAILED          // 失败
        }
    }
    
    /**
     * 批次级别断点
     */
    @Data
    public static class BatchCheckpoint {
        private long batchNumber;
        private int batchSize;
        private List<String> batchDocIds;
        private long processedInBatch;
        private long successInBatch;
        private long failedInBatch;
        private Map<String, String> batchMetadata; // 批次元数据
    }
    
    /**
     * 分片级别断点  
     */
    @Data
    public static class ShardCheckpoint {
        private int shardIndex;
        private long shardStartOffset;
        private long shardEndOffset;
        private long currentOffset;
        private String shardQuery;  // 分片查询条件
        private Map<String, Object> shardContext; // 分片上下文
    }
    
    /**
     * 时间范围断点
     */
    @Data
    public static class TimeRangeCheckpoint {
        private Date startTime;
        private Date endTime;
        private Date currentTime;
        private int timeWindowMinutes;
        private String timeField;
        private long processedTimeRanges;
        private Date lastProcessedTime;
    }
    
    /**
     * 检查是否可以重试
     */
    public boolean canRetry() {
        return retryCount < maxRetries;
    }
    
    /**
     * 增加重试次数
     */
    public void incrementRetry() {
        this.retryCount++;
    }
    
    /**
     * 重置重试次数
     */
    public void resetRetry() {
        this.retryCount = 0;
    }
    
    /**
     * 检查断点是否过期
     */
    public boolean isExpired(long expirationTimeMs) {
        if (lastUpdateTime == null) return false;
        return System.currentTimeMillis() - lastUpdateTime.getTime() > expirationTimeMs;
    }
    
    /**
     * 更新断点时间
     */
    public void updateTimestamp() {
        this.lastUpdateTime = new Date();
    }
    
    /**
     * 计算完成进度
     */
    public double getProgress() {
        if (batchCheckpoint != null && batchCheckpoint.getBatchDocIds() != null) {
            int totalDocs = batchCheckpoint.getBatchDocIds().size();
            if (totalDocs == 0) return 0.0;
            return (double) batchCheckpoint.getProcessedInBatch() / totalDocs * 100.0;
        }
        return 0.0;
    }
}