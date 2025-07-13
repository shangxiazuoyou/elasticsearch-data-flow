package com.everflowx.esmigration.domain;

import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 索引同步结果类
 * 
 * @author everflowx
 */
@Data
public class IndexSyncResult {
    
    /**
     * 同步开始时间
     */
    private Date startTime = new Date();
    
    /**
     * 同步结束时间
     */
    private Date endTime;
    
    /**
     * 总耗时（毫秒）
     */
    private Long durationMs;
    
    /**
     * 同步的索引总数
     */
    private Integer totalIndices = 0;
    
    /**
     * 成功同步的索引数
     */
    private Integer successIndices = 0;
    
    /**
     * 失败的索引数
     */
    private Integer failedIndices = 0;
    
    /**
     * 跳过的索引数
     */
    private Integer skippedIndices = 0;
    
    /**
     * 总文档数
     */
    private Long totalDocuments = 0L;
    
    /**
     * 成功同步的文档数
     */
    private Long successDocuments = 0L;
    
    /**
     * 失败的文档数
     */
    private Long failedDocuments = 0L;
    
    /**
     * 每个索引的同步详情
     */
    private Map<String, IndexSyncDetail> indexDetails = new ConcurrentHashMap<>();
    
    /**
     * 错误信息列表
     */
    private List<String> errors = new ArrayList<>();
    
    /**
     * 警告信息列表
     */
    private List<String> warnings = new ArrayList<>();
    
    /**
     * 是否同步成功
     */
    private Boolean success = true;
    
    /**
     * 同步状态
     */
    private String status = "RUNNING";
    
    /**
     * 总体错误信息
     */
    private String errorMessage;
    
    /**
     * 索引同步详情内部类
     */
    @Data
    public static class IndexSyncDetail {
        private String indexName;
        private String status; // SUCCESS, FAILED, SKIPPED
        private Long documentCount = 0L;
        private Long successCount = 0L;
        private Long failedCount = 0L;
        private Date startTime;
        private Date endTime;
        private Long durationMs;
        private String errorMessage;
        private Boolean settingsSynced = false;
        private Boolean mappingsSynced = false;
        private Boolean aliasesSynced = false;
        private Boolean dataSynced = false;
    }
    
    /**
     * 添加索引同步详情
     */
    public void addIndexDetail(String indexName, IndexSyncDetail detail) {
        this.indexDetails.put(indexName, detail);
        this.totalIndices++;
        
        if ("SUCCESS".equals(detail.getStatus())) {
            this.successIndices++;
            this.successDocuments += detail.getSuccessCount();
        } else if ("FAILED".equals(detail.getStatus())) {
            this.failedIndices++;
            this.failedDocuments += detail.getFailedCount();
        } else if ("SKIPPED".equals(detail.getStatus())) {
            this.skippedIndices++;
        }
        
        this.totalDocuments += detail.getDocumentCount();
    }
    
    /**
     * 添加错误信息
     */
    public void addError(String error) {
        this.errors.add(error);
        this.success = false;
    }
    
    /**
     * 添加警告信息
     */
    public void addWarning(String warning) {
        this.warnings.add(warning);
    }
    
    /**
     * 完成同步
     */
    public void finish() {
        this.endTime = new Date();
        this.durationMs = this.endTime.getTime() - this.startTime.getTime();
        
        if (this.success && this.failedIndices == 0) {
            this.status = "SUCCESS";
        } else if (this.successIndices > 0) {
            this.status = "PARTIAL_SUCCESS";
        } else {
            this.status = "FAILED";
            this.success = false;
        }
    }
    
    /**
     * 获取成功率
     */
    public double getSuccessRate() {
        if (totalIndices == 0) return 0.0;
        return (double) successIndices / totalIndices * 100;
    }
    
    /**
     * 获取文档同步成功率
     */
    public double getDocumentSuccessRate() {
        if (totalDocuments == 0) return 0.0;
        return (double) successDocuments / totalDocuments * 100;
    }
}
