package com.everflowx.esmigration.checkpoint;

import lombok.Data;
import java.util.List;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;

/**
 * 恢复步骤
 * 定义单个恢复步骤的详细信息和执行状态
 * 
 * @author everflowx
 */
@Data
public class ResumeStep {
    
    /**
     * 步骤ID
     */
    private String stepId;
    
    /**
     * 步骤类型
     */
    private StepType stepType;
    
    /**
     * 相关断点列表
     */
    private List<FieldLevelCheckpoint> checkpoints;
    
    /**
     * 执行优先级（数字越小优先级越高）
     */
    private int priority;
    
    /**
     * 步骤状态
     */
    private StepStatus status = StepStatus.PENDING;
    
    /**
     * 开始时间
     */
    private Date startTime;
    
    /**
     * 完成时间
     */
    private Date completeTime;
    
    /**
     * 已处理的断点数
     */
    private int processedCheckpoints = 0;
    
    /**
     * 成功处理的断点数
     */
    private int successfulCheckpoints = 0;
    
    /**
     * 失败的断点数
     */
    private int failedCheckpoints = 0;
    
    /**
     * 错误信息
     */
    private String errorMessage;
    
    /**
     * 步骤元数据
     */
    private Map<String, Object> metadata = new HashMap<>();
    
    /**
     * 预计执行时间（毫秒）
     */
    private long estimatedDurationMs;
    
    /**
     * 实际执行时间（毫秒）
     */
    private long actualDurationMs;
    
    /**
     * 步骤类型枚举
     */
    public enum StepType {
        DOCUMENT_RECOVERY("文档级别恢复", "恢复失败的文档处理"),
        FIELD_RECOVERY("字段级别恢复", "恢复失败的字段转换"),
        BATCH_RECOVERY("批次级别恢复", "恢复失败的批次处理"),
        SHARD_RECOVERY("分片级别恢复", "恢复失败的分片迁移"),
        TIME_RANGE_RECOVERY("时间范围恢复", "恢复基于时间范围的迁移"),
        VALIDATION_RECOVERY("验证恢复", "恢复数据验证步骤"),
        INDEX_RECOVERY("索引恢复", "恢复索引操作");
        
        private final String displayName;
        private final String description;
        
        StepType(String displayName, String description) {
            this.displayName = displayName;
            this.description = description;
        }
        
        public String getDisplayName() { return displayName; }
        public String getDescription() { return description; }
    }
    
    /**
     * 步骤状态枚举
     */
    public enum StepStatus {
        PENDING,     // 待执行
        EXECUTING,   // 执行中
        COMPLETED,   // 已完成
        FAILED,      // 失败
        SKIPPED,     // 已跳过
        CANCELLED    // 已取消
    }
    
    /**
     * 开始执行步骤
     */
    public void start() {
        this.status = StepStatus.EXECUTING;
        this.startTime = new Date();
        this.processedCheckpoints = 0;
        this.successfulCheckpoints = 0;
        this.failedCheckpoints = 0;
    }
    
    /**
     * 完成步骤执行
     */
    public void complete() {
        this.status = StepStatus.COMPLETED;
        this.completeTime = new Date();
        this.actualDurationMs = getExecutionTimeMs();
    }
    
    /**
     * 标记步骤失败
     */
    public void fail(String errorMessage) {
        this.status = StepStatus.FAILED;
        this.errorMessage = errorMessage;
        this.completeTime = new Date();
        this.actualDurationMs = getExecutionTimeMs();
    }
    
    /**
     * 跳过步骤
     */
    public void skip(String reason) {
        this.status = StepStatus.SKIPPED;
        this.errorMessage = reason;
        this.completeTime = new Date();
        this.actualDurationMs = getExecutionTimeMs();
    }
    
    /**
     * 取消步骤
     */
    public void cancel() {
        this.status = StepStatus.CANCELLED;
        this.completeTime = new Date();
        this.actualDurationMs = getExecutionTimeMs();
    }
    
    /**
     * 更新处理进度
     */
    public void updateProgress(int processed, int successful, int failed) {
        this.processedCheckpoints = processed;
        this.successfulCheckpoints = successful;
        this.failedCheckpoints = failed;
    }
    
    /**
     * 增加成功处理数
     */
    public void incrementSuccess() {
        this.processedCheckpoints++;
        this.successfulCheckpoints++;
    }
    
    /**
     * 增加失败处理数
     */
    public void incrementFailure() {
        this.processedCheckpoints++;
        this.failedCheckpoints++;
    }
    
    /**
     * 获取执行进度百分比
     */
    public double getProgress() {
        if (checkpoints == null || checkpoints.isEmpty()) {
            return status == StepStatus.COMPLETED ? 100.0 : 0.0;
        }
        return (double) processedCheckpoints / checkpoints.size() * 100.0;
    }
    
    /**
     * 获取成功率
     */
    public double getSuccessRate() {
        if (processedCheckpoints == 0) {
            return 0.0;
        }
        return (double) successfulCheckpoints / processedCheckpoints * 100.0;
    }
    
    /**
     * 获取执行时间
     */
    public long getExecutionTimeMs() {
        if (startTime == null) {
            return 0;
        }
        Date endTime = completeTime != null ? completeTime : new Date();
        return endTime.getTime() - startTime.getTime();
    }
    
    /**
     * 检查是否需要执行
     */
    public boolean needsExecution() {
        return checkpoints != null && !checkpoints.isEmpty() && 
               status == StepStatus.PENDING;
    }
    
    /**
     * 检查是否可以重试
     */
    public boolean canRetry() {
        return status == StepStatus.FAILED && failedCheckpoints > 0;
    }
    
    /**
     * 获取剩余的断点数
     */
    public int getRemainingCheckpoints() {
        if (checkpoints == null) {
            return 0;
        }
        return checkpoints.size() - processedCheckpoints;
    }
    
    /**
     * 获取预计剩余时间
     */
    public long getEstimatedRemainingTimeMs() {
        if (processedCheckpoints == 0 || checkpoints == null || checkpoints.isEmpty()) {
            return estimatedDurationMs;
        }
        
        long currentDuration = getExecutionTimeMs();
        double avgTimePerCheckpoint = (double) currentDuration / processedCheckpoints;
        int remainingCheckpoints = getRemainingCheckpoints();
        
        return (long) (avgTimePerCheckpoint * remainingCheckpoints);
    }
    
    /**
     * 添加元数据
     */
    public void addMetadata(String key, Object value) {
        if (metadata == null) {
            metadata = new HashMap<>();
        }
        metadata.put(key, value);
    }
    
    /**
     * 获取元数据
     */
    public Object getMetadata(String key) {
        return metadata != null ? metadata.get(key) : null;
    }
    
    /**
     * 生成步骤摘要
     */
    public String generateSummary() {
        StringBuilder summary = new StringBuilder();
        summary.append(String.format("%s [%s]:\\n", stepType.getDisplayName(), status));
        summary.append(String.format("- 描述: %s\\n", stepType.getDescription()));
        summary.append(String.format("- 优先级: %d\\n", priority));
        summary.append(String.format("- 断点总数: %d\\n", checkpoints != null ? checkpoints.size() : 0));
        summary.append(String.format("- 已处理: %d\\n", processedCheckpoints));
        summary.append(String.format("- 成功: %d\\n", successfulCheckpoints));
        summary.append(String.format("- 失败: %d\\n", failedCheckpoints));
        summary.append(String.format("- 进度: %.1f%%\\n", getProgress()));
        summary.append(String.format("- 成功率: %.1f%%\\n", getSuccessRate()));
        
        if (startTime != null) {
            summary.append(String.format("- 执行时间: %d ms\\n", getExecutionTimeMs()));
            
            if (status == StepStatus.EXECUTING) {
                summary.append(String.format("- 预计剩余时间: %d ms\\n", getEstimatedRemainingTimeMs()));
            }
        }
        
        if (errorMessage != null) {
            summary.append(String.format("- 错误信息: %s\\n", errorMessage));
        }
        
        return summary.toString();
    }
    
    /**
     * 创建重试步骤
     */
    public ResumeStep createRetryStep() {
        if (!canRetry()) {
            return null;
        }
        
        ResumeStep retryStep = new ResumeStep();
        retryStep.setStepId(this.stepId + "_retry_" + System.currentTimeMillis());
        retryStep.setStepType(this.stepType);
        retryStep.setPriority(this.priority + 100); // 重试步骤优先级较低
        
        // 只包含失败的断点
        List<FieldLevelCheckpoint> failedCheckpoints = this.checkpoints.stream()
            .filter(cp -> cp.getStatus() == FieldLevelCheckpoint.CheckpointStatus.FAILED)
            .filter(FieldLevelCheckpoint::canRetry)
            .collect(java.util.stream.Collectors.toList());
        
        retryStep.setCheckpoints(failedCheckpoints);
        retryStep.addMetadata("retry_of", this.stepId);
        retryStep.addMetadata("original_failed_count", this.failedCheckpoints);
        
        return retryStep;
    }
}