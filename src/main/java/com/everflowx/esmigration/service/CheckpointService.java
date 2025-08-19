package com.everflowx.esmigration.service;

import com.everflowx.esmigration.domain.MigrationCheckpoint;
import com.everflowx.esmigration.domain.MigrationConfig;

import java.util.List;
import java.util.Optional;

/**
 * 断点续传服务接口
 * 
 * @author everflowx
 */
public interface CheckpointService {
    
    /**
     * 保存断点信息
     */
    void saveCheckpoint(MigrationCheckpoint checkpoint);
    
    /**
     * 获取断点信息
     */
    Optional<MigrationCheckpoint> getCheckpoint(String taskId);
    
    /**
     * 获取分片断点信息
     */
    Optional<MigrationCheckpoint> getShardCheckpoint(String taskId, int shardIndex);
    
    /**
     * 删除断点信息
     */
    void removeCheckpoint(String taskId);
    
    /**
     * 删除分片断点信息
     */
    void removeShardCheckpoint(String taskId, int shardIndex);
    
    /**
     * 获取所有未完成的断点
     */
    List<MigrationCheckpoint> getUnfinishedCheckpoints();
    
    /**
     * 更新断点进度
     */
    void updateProgress(String taskId, long processedCount, long successCount, long failedCount);
    
    /**
     * 更新分片断点进度
     */
    void updateShardProgress(String taskId, int shardIndex, long processedCount, long successCount, long failedCount);
    
    /**
     * 检查任务是否可以从断点恢复
     */
    boolean canResumeFromCheckpoint(String taskId, MigrationConfig config);
    
    /**
     * 清理过期断点（超过24小时的失败任务）
     */
    void cleanupExpiredCheckpoints();
}