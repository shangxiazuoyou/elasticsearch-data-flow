package com.everflowx.esmigration.domain;

import lombok.Data;

import java.util.Date;

/**
 * 迁移断点信息
 * 
 * @author everflowx
 */
@Data
public class MigrationCheckpoint {
    
    /**
     * 任务ID
     */
    private String taskId;
    
    /**
     * 源索引名
     */
    private String sourceIndex;
    
    /**
     * 目标索引名
     */
    private String targetIndex;
    
    /**
     * 当前scroll ID
     */
    private String scrollId;
    
    /**
     * 已处理文档数量
     */
    private long processedCount;
    
    /**
     * 已成功文档数量
     */
    private long successCount;
    
    /**
     * 已失败文档数量
     */
    private long failedCount;
    
    /**
     * 总文档数量
     */
    private long totalCount;
    
    /**
     * 当前批次大小
     */
    private int currentBatchSize;
    
    /**
     * 分片索引（并行迁移时使用）
     */
    private Integer shardIndex;
    
    /**
     * 分片范围开始
     */
    private Long shardStartOffset;
    
    /**
     * 分片范围结束
     */
    private Long shardEndOffset;
    
    /**
     * 最后更新时间
     */
    private Date lastUpdateTime;
    
    /**
     * 迁移配置快照（JSON格式）
     */
    private String configSnapshot;
    
    /**
     * 错误信息
     */
    private String errorMessage;
    
    /**
     * 迁移状态
     */
    private String status;
    
    /**
     * 线程名称
     */
    private String threadName;
    
    /**
     * 创建时间
     */
    private Date createTime;
}