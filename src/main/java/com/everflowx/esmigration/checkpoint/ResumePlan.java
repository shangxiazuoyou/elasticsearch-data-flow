package com.everflowx.esmigration.checkpoint;

import lombok.Data;
import java.util.List;
import java.util.Date;

/**
 * 断点恢复计划
 * 定义了如何从断点恢复迁移任务的详细步骤
 * 
 * @author everflowx
 */
@Data
public class ResumePlan {
    
    /**
     * 任务ID
     */
    private String taskId;
    
    /**
     * 恢复步骤列表
     */
    private List<ResumeStep> steps;
    
    /**
     * 总断点数
     */
    private int totalCheckpoints;
    
    /**
     * 预计恢复时间（毫秒）
     */
    private long estimatedRecoveryTimeMs;
    
    /**
     * 创建时间
     */
    private Date createTime = new Date();
    
    /**
     * 计划状态
     */
    private PlanStatus status = PlanStatus.CREATED;
    
    /**
     * 当前执行步骤索引
     */
    private int currentStepIndex = 0;
    
    /**
     * 开始执行时间
     */
    private Date startTime;
    
    /**
     * 完成时间
     */
    private Date completeTime;
    
    /**
     * 错误信息
     */
    private String errorMessage;
    
    /**
     * 计划状态枚举
     */
    public enum PlanStatus {
        CREATED,     // 已创建
        EXECUTING,   // 执行中
        COMPLETED,   // 已完成
        FAILED,      // 失败
        CANCELLED    // 已取消
    }
    
    /**
     * 获取当前步骤
     */
    public ResumeStep getCurrentStep() {
        if (steps != null && currentStepIndex < steps.size()) {
            return steps.get(currentStepIndex);
        }
        return null;
    }
    
    /**
     * 移动到下一步
     */
    public boolean moveToNextStep() {
        if (steps != null && currentStepIndex < steps.size() - 1) {
            currentStepIndex++;
            return true;
        }
        return false;
    }
    
    /**
     * 检查是否完成
     */
    public boolean isCompleted() {
        return currentStepIndex >= (steps != null ? steps.size() : 0);
    }
    
    /**
     * 获取总体进度百分比
     */
    public double getOverallProgress() {
        if (steps == null || steps.isEmpty()) {
            return 0.0;
        }
        
        double totalProgress = 0.0;
        for (ResumeStep step : steps) {
            totalProgress += step.getProgress();
        }
        
        return totalProgress / steps.size();
    }
    
    /**
     * 开始执行计划
     */
    public void start() {
        this.status = PlanStatus.EXECUTING;
        this.startTime = new Date();
        this.currentStepIndex = 0;
    }
    
    /**
     * 完成计划执行
     */
    public void complete() {
        this.status = PlanStatus.COMPLETED;
        this.completeTime = new Date();
    }
    
    /**
     * 标记计划失败
     */
    public void fail(String errorMessage) {
        this.status = PlanStatus.FAILED;
        this.errorMessage = errorMessage;
        this.completeTime = new Date();
    }
    
    /**
     * 取消计划执行
     */
    public void cancel() {
        this.status = PlanStatus.CANCELLED;
        this.completeTime = new Date();
    }
    
    /**
     * 获取执行耗时
     */
    public long getExecutionTimeMs() {
        if (startTime == null) {
            return 0;
        }
        
        Date endTime = completeTime != null ? completeTime : new Date();
        return endTime.getTime() - startTime.getTime();
    }
    
    /**
     * 生成恢复计划摘要
     */
    public String generateSummary() {
        StringBuilder summary = new StringBuilder();
        summary.append(String.format("恢复计划 [%s]:\\n", taskId));
        summary.append(String.format("- 状态: %s\\n", status));
        summary.append(String.format("- 总步骤数: %d\\n", steps != null ? steps.size() : 0));
        summary.append(String.format("- 当前步骤: %d/%d\\n", currentStepIndex + 1, steps != null ? steps.size() : 0));
        summary.append(String.format("- 总体进度: %.1f%%\\n", getOverallProgress()));
        summary.append(String.format("- 总断点数: %d\\n", totalCheckpoints));
        
        if (startTime != null) {
            summary.append(String.format("- 执行耗时: %d ms\\n", getExecutionTimeMs()));
        }
        
        if (errorMessage != null) {
            summary.append(String.format("- 错误信息: %s\\n", errorMessage));
        }
        
        if (steps != null) {
            summary.append("\\n步骤详情:\\n");
            for (int i = 0; i < steps.size(); i++) {
                ResumeStep step = steps.get(i);
                String status = i < currentStepIndex ? "✓" : i == currentStepIndex ? "→" : "○";
                summary.append(String.format("  %s 步骤%d: %s (优先级:%d, 进度:%.1f%%, 断点数:%d)\\n", 
                    status, i + 1, step.getStepType(), step.getPriority(), 
                    step.getProgress(), step.getCheckpoints() != null ? step.getCheckpoints().size() : 0));
            }
        }
        
        return summary.toString();
    }
}