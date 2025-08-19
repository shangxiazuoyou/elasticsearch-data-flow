package com.everflowx.esmigration.service.impl;

import com.alibaba.fastjson.JSON;
import com.everflowx.esmigration.domain.MigrationCheckpoint;
import com.everflowx.esmigration.domain.MigrationConfig;
import com.everflowx.esmigration.service.CheckpointService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 断点续传服务实现类 - 基于文件存储
 * 
 * @author everflowx
 */
@Slf4j
@Service
public class CheckpointServiceImpl implements CheckpointService {
    
    @Value("${es.migration.checkpoint.dir:./checkpoints}")
    private String checkpointDir;
    
    // 内存缓存，提高读取性能
    private final ConcurrentHashMap<String, MigrationCheckpoint> checkpointCache = new ConcurrentHashMap<>();
    
    @Override
    public void saveCheckpoint(MigrationCheckpoint checkpoint) {
        try {
            ensureCheckpointDirectory();
            
            checkpoint.setLastUpdateTime(new Date());
            if (checkpoint.getCreateTime() == null) {
                checkpoint.setCreateTime(new Date());
            }
            
            String fileName = getCheckpointFileName(checkpoint.getTaskId(), checkpoint.getShardIndex());
            Path filePath = Paths.get(checkpointDir, fileName);
            
            String checkpointJson = JSON.toJSONString(checkpoint, true);
            Files.write(filePath, checkpointJson.getBytes(), 
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            
            // 更新内存缓存
            String cacheKey = getCacheKey(checkpoint.getTaskId(), checkpoint.getShardIndex());
            checkpointCache.put(cacheKey, checkpoint);
            
            log.debug("保存断点信息: {}", fileName);
            
        } catch (Exception e) {
            log.error("保存断点信息失败: {}", checkpoint.getTaskId(), e);
        }
    }
    
    @Override
    public Optional<MigrationCheckpoint> getCheckpoint(String taskId) {
        return getCheckpointInternal(taskId, null);
    }
    
    @Override
    public Optional<MigrationCheckpoint> getShardCheckpoint(String taskId, int shardIndex) {
        return getCheckpointInternal(taskId, shardIndex);
    }
    
    private Optional<MigrationCheckpoint> getCheckpointInternal(String taskId, Integer shardIndex) {
        try {
            String cacheKey = getCacheKey(taskId, shardIndex);
            
            // 先从内存缓存获取
            MigrationCheckpoint cachedCheckpoint = checkpointCache.get(cacheKey);
            if (cachedCheckpoint != null) {
                return Optional.of(cachedCheckpoint);
            }
            
            // 从文件获取
            String fileName = getCheckpointFileName(taskId, shardIndex);
            Path filePath = Paths.get(checkpointDir, fileName);
            
            if (!Files.exists(filePath)) {
                return Optional.empty();
            }
            
            String checkpointJson = new String(Files.readAllBytes(filePath));
            MigrationCheckpoint checkpoint = JSON.parseObject(checkpointJson, MigrationCheckpoint.class);
            
            // 更新内存缓存
            checkpointCache.put(cacheKey, checkpoint);
            
            log.debug("加载断点信息: {}", fileName);
            return Optional.of(checkpoint);
            
        } catch (Exception e) {
            log.error("获取断点信息失败: {} - {}", taskId, shardIndex, e);
            return Optional.empty();
        }
    }
    
    @Override
    public void removeCheckpoint(String taskId) {
        removeCheckpointInternal(taskId, null);
    }
    
    @Override
    public void removeShardCheckpoint(String taskId, int shardIndex) {
        removeCheckpointInternal(taskId, shardIndex);
    }
    
    private void removeCheckpointInternal(String taskId, Integer shardIndex) {
        try {
            String fileName = getCheckpointFileName(taskId, shardIndex);
            Path filePath = Paths.get(checkpointDir, fileName);
            
            if (Files.exists(filePath)) {
                Files.delete(filePath);
                log.info("删除断点文件: {}", fileName);
            }
            
            // 从内存缓存移除
            String cacheKey = getCacheKey(taskId, shardIndex);
            checkpointCache.remove(cacheKey);
            
        } catch (Exception e) {
            log.error("删除断点信息失败: {} - {}", taskId, shardIndex, e);
        }
    }
    
    @Override
    public List<MigrationCheckpoint> getUnfinishedCheckpoints() {
        List<MigrationCheckpoint> unfinishedCheckpoints = new ArrayList<>();
        
        try {
            ensureCheckpointDirectory();
            Path checkpointPath = Paths.get(checkpointDir);
            
            Files.list(checkpointPath)
                .filter(path -> path.toString().endsWith(".json"))
                .forEach(path -> {
                    try {
                        String checkpointJson = new String(Files.readAllBytes(path));
                        MigrationCheckpoint checkpoint = JSON.parseObject(checkpointJson, MigrationCheckpoint.class);
                        
                        if (!"COMPLETED".equals(checkpoint.getStatus()) && !"CANCELLED".equals(checkpoint.getStatus())) {
                            unfinishedCheckpoints.add(checkpoint);
                        }
                    } catch (Exception e) {
                        log.warn("读取断点文件失败: {}", path, e);
                    }
                });
                
        } catch (Exception e) {
            log.error("获取未完成断点列表失败", e);
        }
        
        return unfinishedCheckpoints;
    }
    
    @Override
    public void updateProgress(String taskId, long processedCount, long successCount, long failedCount) {
        updateProgressInternal(taskId, null, processedCount, successCount, failedCount);
    }
    
    @Override
    public void updateShardProgress(String taskId, int shardIndex, long processedCount, long successCount, long failedCount) {
        updateProgressInternal(taskId, shardIndex, processedCount, successCount, failedCount);
    }
    
    private void updateProgressInternal(String taskId, Integer shardIndex, long processedCount, long successCount, long failedCount) {
        Optional<MigrationCheckpoint> checkpointOpt = getCheckpointInternal(taskId, shardIndex);
        if (checkpointOpt.isPresent()) {
            MigrationCheckpoint checkpoint = checkpointOpt.get();
            checkpoint.setProcessedCount(processedCount);
            checkpoint.setSuccessCount(successCount);
            checkpoint.setFailedCount(failedCount);
            checkpoint.setStatus("RUNNING");
            saveCheckpoint(checkpoint);
        }
    }
    
    @Override
    public boolean canResumeFromCheckpoint(String taskId, MigrationConfig config) {
        Optional<MigrationCheckpoint> checkpointOpt = getCheckpoint(taskId);
        if (!checkpointOpt.isPresent()) {
            return false;
        }
        
        MigrationCheckpoint checkpoint = checkpointOpt.get();
        
        // 检查配置是否匹配
        if (!checkpoint.getSourceIndex().equals(config.getSourceIndex()) ||
            !checkpoint.getTargetIndex().equals(config.getTargetIndex())) {
            log.warn("断点配置不匹配，无法恢复: 期望 {}->{}, 实际 {}->{}",
                config.getSourceIndex(), config.getTargetIndex(),
                checkpoint.getSourceIndex(), checkpoint.getTargetIndex());
            return false;
        }
        
        // 检查断点时间，超过24小时的断点可能已过期
        Date lastUpdate = checkpoint.getLastUpdateTime();
        if (lastUpdate != null) {
            long hoursSinceUpdate = (System.currentTimeMillis() - lastUpdate.getTime()) / (1000 * 60 * 60);
            if (hoursSinceUpdate > 24) {
                log.warn("断点已过期 ({} 小时前)，建议重新开始迁移", hoursSinceUpdate);
                return false;
            }
        }
        
        return true;
    }
    
    @Override
    public void cleanupExpiredCheckpoints() {
        try {
            ensureCheckpointDirectory();
            Path checkpointPath = Paths.get(checkpointDir);
            
            long expireTime = System.currentTimeMillis() - (24 * 60 * 60 * 1000); // 24小时前
            
            Files.list(checkpointPath)
                .filter(path -> path.toString().endsWith(".json"))
                .forEach(path -> {
                    try {
                        String checkpointJson = new String(Files.readAllBytes(path));
                        MigrationCheckpoint checkpoint = JSON.parseObject(checkpointJson, MigrationCheckpoint.class);
                        
                        Date lastUpdate = checkpoint.getLastUpdateTime();
                        if (lastUpdate != null && lastUpdate.getTime() < expireTime) {
                            if ("FAILED".equals(checkpoint.getStatus()) || "CANCELLED".equals(checkpoint.getStatus())) {
                                Files.delete(path);
                                log.info("清理过期断点文件: {}", path.getFileName());
                                
                                // 从缓存移除
                                String cacheKey = getCacheKey(checkpoint.getTaskId(), checkpoint.getShardIndex());
                                checkpointCache.remove(cacheKey);
                            }
                        }
                    } catch (Exception e) {
                        log.warn("清理断点文件失败: {}", path, e);
                    }
                });
                
        } catch (Exception e) {
            log.error("清理过期断点失败", e);
        }
    }
    
    /**
     * 确保断点目录存在
     */
    private void ensureCheckpointDirectory() throws IOException {
        Path path = Paths.get(checkpointDir);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
            log.info("创建断点目录: {}", checkpointDir);
        }
    }
    
    /**
     * 生成断点文件名
     */
    private String getCheckpointFileName(String taskId, Integer shardIndex) {
        if (shardIndex != null) {
            return String.format("checkpoint_%s_shard_%d.json", taskId, shardIndex);
        } else {
            return String.format("checkpoint_%s.json", taskId);
        }
    }
    
    /**
     * 生成缓存键
     */
    private String getCacheKey(String taskId, Integer shardIndex) {
        if (shardIndex != null) {
            return taskId + "_shard_" + shardIndex;
        } else {
            return taskId;
        }
    }
}