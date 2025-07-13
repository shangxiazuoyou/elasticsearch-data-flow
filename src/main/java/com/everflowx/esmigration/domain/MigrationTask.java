package com.everflowx.esmigration.domain;

import lombok.Data;

import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 迁移任务实体
 * 
 * @author everflowx
 */
@Data
public class MigrationTask {
    
    /**
     * 任务ID
     */
    private String taskId;
    
    /**
     * 任务名称
     */
    private String taskName;
    
    /**
     * 任务类型
     */
    private String taskType; // FULL_MIGRATION, INCREMENTAL_MIGRATION, INDEX_SYNC
    
    /**
     * 任务状态
     */
    private TaskStatus status = TaskStatus.PENDING;
    
    /**
     * 迁移配置
     */
    private MigrationConfig migrationConfig;
    
    /**
     * 索引同步配置
     */
    private IndexSyncConfig indexSyncConfig;
    
    /**
     * 创建时间
     */
    private Date createTime = new Date();
    
    /**
     * 开始时间
     */
    private Date startTime;
    
    /**
     * 结束时间
     */
    private Date endTime;
    
    /**
     * 总文档数
     */
    private AtomicLong totalDocuments = new AtomicLong(0);
    
    /**
     * 已处理文档数
     */
    private AtomicLong processedDocuments = new AtomicLong(0);
    
    /**
     * 成功文档数
     */
    private AtomicLong successDocuments = new AtomicLong(0);
    
    /**
     * 失败文档数
     */
    private AtomicLong failedDocuments = new AtomicLong(0);
    
    /**
     * 跳过文档数
     */
    private AtomicLong skippedDocuments = new AtomicLong(0);
    
    /**
     * 当前处理的批次号
     */
    private AtomicLong currentBatch = new AtomicLong(0);
    
    /**
     * 总批次数
     */
    private AtomicLong totalBatches = new AtomicLong(0);
    
    /**
     * 当前处理速度（文档/秒）
     */
    private AtomicLong currentSpeed = new AtomicLong(0);
    
    /**
     * 平均处理速度（文档/秒）
     */
    private AtomicLong averageSpeed = new AtomicLong(0);
    
    /**
     * 预计剩余时间（秒）
     */
    private AtomicLong estimatedRemainingTime = new AtomicLong(0);
    
    /**
     * 错误信息
     */
    private String errorMessage;
    
    /**
     * 错误详情列表
     */
    private List<String> errorDetails;
    
    /**
     * 当前阶段描述
     */
    private String currentPhase;
    
    /**
     * 进度百分比（0-100）
     */
    private volatile double progressPercentage = 0.0;
    
    /**
     * 最后更新时间
     */
    private Date lastUpdateTime = new Date();
    
    /**
     * 任务创建者
     */
    private String creator = "system";
    
    /**
     * 获取运行时长（毫秒）
     */
    public long getDurationMs() {
        if (startTime == null) {
            return 0;
        }
        Date endTimeToUse = endTime != null ? endTime : new Date();
        return endTimeToUse.getTime() - startTime.getTime();
    }
    
    /**
     * 更新进度
     */
    public void updateProgress() {
        long total = totalDocuments.get();
        long processed = processedDocuments.get();
        
        if (total > 0) {
            this.progressPercentage = (double) processed / total * 100.0;
        }
        
        // 计算处理速度和预计剩余时间
        long durationMs = getDurationMs();
        if (durationMs > 0 && processed > 0) {
            long avgSpeed = processed * 1000 / durationMs; // 文档/秒
            this.averageSpeed.set(avgSpeed);
            
            long remaining = total - processed;
            if (avgSpeed > 0) {
                this.estimatedRemainingTime.set(remaining / avgSpeed);
            }
        }
        
        this.lastUpdateTime = new Date();
    }
    
    /**
     * 是否正在运行
     */
    public boolean isRunning() {
        return status == TaskStatus.RUNNING;
    }
    
    /**
     * 是否已完成（成功或失败）
     */
    public boolean isFinished() {
        return status == TaskStatus.COMPLETED || 
               status == TaskStatus.FAILED || 
               status == TaskStatus.CANCELLED;
    }
    
    /**
     * 获取简化的任务信息（用于列表显示）
     */
    public MigrationTaskSummary toSummary() {
        MigrationTaskSummary summary = new MigrationTaskSummary();
        summary.setTaskId(this.taskId);
        summary.setTaskName(this.taskName);
        summary.setTaskType(this.taskType);
        summary.setStatus(this.status);
        summary.setCreateTime(this.createTime);
        summary.setStartTime(this.startTime);
        summary.setEndTime(this.endTime);
        summary.setProgressPercentage(this.progressPercentage);
        summary.setTotalDocuments(this.totalDocuments.get());
        summary.setProcessedDocuments(this.processedDocuments.get());
        summary.setDurationMs(this.getDurationMs());
        summary.setCurrentPhase(this.currentPhase);
        return summary;
    }
    
    /**
     * 任务摘要信息
     */
    @Data
    public static class MigrationTaskSummary {
        private String taskId;
        private String taskName;
        private String taskType;
        private TaskStatus status;
        private Date createTime;
        private Date startTime;
        private Date endTime;
        private double progressPercentage;
        private long totalDocuments;
        private long processedDocuments;
        private long durationMs;
        private String currentPhase;
    }
}