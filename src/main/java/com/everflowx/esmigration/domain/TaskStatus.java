package com.everflowx.esmigration.domain;

/**
 * 任务状态枚举
 * 
 * @author everflowx
 */
public enum TaskStatus {
    PENDING("待执行"),
    RUNNING("执行中"),
    COMPLETED("已完成"),
    FAILED("执行失败"),
    CANCELLED("已取消"),
    PAUSED("已暂停");
    
    private final String description;
    
    TaskStatus(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return description;
    }
}