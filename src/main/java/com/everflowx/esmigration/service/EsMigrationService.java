package com.everflowx.esmigration.service;

import com.everflowx.esmigration.domain.MigrationConfig;

import java.util.Date;
import java.util.List;

/**
 * ES数据迁移服务接口
 * 
 * @author everflowx
 */
public interface EsMigrationService {
    
    /**
     * 全量数据迁移
     * 
     * @param config 迁移配置
     * @return 迁移结果统计
     */
    MigrationResult fullMigration(MigrationConfig config);
    
    /**
     * 增量数据迁移
     * 
     * @param config 迁移配置
     * @param lastSyncTime 上次同步时间
     * @return 迁移结果统计
     */
    MigrationResult incrementalMigration(MigrationConfig config, Date lastSyncTime);
    
    /**
     * 检查索引是否存在
     * 
     * @param indexName 索引名称
     * @param isTarget 是否为目标ES
     * @return 是否存在
     */
    boolean indexExists(String indexName, boolean isTarget);
    
    /**
     * 创建索引
     * 
     * @param indexName 索引名称
     * @param mapping 索引映射
     * @param isTarget 是否为目标ES
     * @return 是否创建成功
     */
    boolean createIndex(String indexName, String mapping, boolean isTarget);
    
    /**
     * 获取索引映射
     * 
     * @param indexName 索引名称
     * @param isTarget 是否为目标ES
     * @return 索引映射JSON
     */
    String getIndexMapping(String indexName, boolean isTarget);
    
    /**
     * 获取文档总数
     * 
     * @param indexName 索引名称
     * @param isTarget 是否为目标ES
     * @return 文档总数
     */
    long getDocumentCount(String indexName, boolean isTarget);
    
    /**
     * 验证迁移结果
     * 
     * @param config 迁移配置
     * @return 验证结果
     */
    ValidationResult validateMigration(MigrationConfig config);
    
    /**
     * 获取最后同步时间
     * 
     * @param indexName 索引名称
     * @param timestampField 时间字段名
     * @return 最后同步时间
     */
    Date getLastSyncTime(String indexName, String timestampField);
    
    /**
     * 迁移结果统计
     */
    class MigrationResult {
        private long totalCount;
        private long successCount;
        private long failedCount;
        private long skippedCount;
        private Date startTime;
        private Date endTime;
        private String errorMessage;
        private List<String> errors;
        
        // 构造函数、getter和setter
        public MigrationResult() {
            this.startTime = new Date();
        }
        
        public void finish() {
            this.endTime = new Date();
        }
        
        public long getDurationMs() {
            if (startTime != null && endTime != null) {
                return endTime.getTime() - startTime.getTime();
            }
            return 0;
        }
        
        // Getters and Setters
        public long getTotalCount() { return totalCount; }
        public void setTotalCount(long totalCount) { this.totalCount = totalCount; }
        
        public long getSuccessCount() { return successCount; }
        public void setSuccessCount(long successCount) { this.successCount = successCount; }
        
        public long getFailedCount() { return failedCount; }
        public void setFailedCount(long failedCount) { this.failedCount = failedCount; }
        
        public long getSkippedCount() { return skippedCount; }
        public void setSkippedCount(long skippedCount) { this.skippedCount = skippedCount; }
        
        public Date getStartTime() { return startTime; }
        public void setStartTime(Date startTime) { this.startTime = startTime; }
        
        public Date getEndTime() { return endTime; }
        public void setEndTime(Date endTime) { this.endTime = endTime; }
        
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        
        public List<String> getErrors() { return errors; }
        public void setErrors(List<String> errors) { this.errors = errors; }

        public boolean isSuccess() {
            return errorMessage == null && (errors == null || errors.isEmpty());
        }
    }
    
    /**
     * 验证结果
     */
    class ValidationResult {
        private boolean success;
        private long sourceCount;
        private long targetCount;
        private String message;
        private List<String> differences;
        
        // Getters and Setters
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
        
        public long getSourceCount() { return sourceCount; }
        public void setSourceCount(long sourceCount) { this.sourceCount = sourceCount; }
        
        public long getTargetCount() { return targetCount; }
        public void setTargetCount(long targetCount) { this.targetCount = targetCount; }
        
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        
        public List<String> getDifferences() { return differences; }
        public void setDifferences(List<String> differences) { this.differences = differences; }
    }
}
