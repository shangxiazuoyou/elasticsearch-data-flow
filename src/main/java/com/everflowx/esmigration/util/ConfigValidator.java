package com.everflowx.esmigration.util;

import com.everflowx.esmigration.domain.IndexSyncConfig;
import com.everflowx.esmigration.domain.MigrationConfig;
import com.everflowx.esmigration.exception.MigrationConfigException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 配置验证工具类
 * 
 * @author everflowx
 */
@Slf4j
@Component
public class ConfigValidator {
    
    /**
     * 验证迁移配置
     */
    public void validateMigrationConfig(MigrationConfig config) {
        if (config == null) {
            throw new MigrationConfigException("迁移配置不能为空");
        }
        
        // 验证索引名称
        if (!StringUtils.hasText(config.getSourceIndex())) {
            throw MigrationConfigException.missingRequiredField("sourceIndex");
        }
        if (!StringUtils.hasText(config.getTargetIndex())) {
            throw MigrationConfigException.missingRequiredField("targetIndex");
        }
        
        // 验证批次大小
        if (config.getBatchSize() == null || config.getBatchSize() < 1 || config.getBatchSize() > 10000) {
            throw MigrationConfigException.invalidBatchSize(config.getBatchSize() != null ? config.getBatchSize() : 0);
        }
        
        // 验证滚动超时时间
        if (config.getScrollTimeout() == null || config.getScrollTimeout() < 1 || config.getScrollTimeout() > 60) {
            throw MigrationConfigException.invalidScrollTimeout(config.getScrollTimeout() != null ? config.getScrollTimeout() : 0);
        }
        
        // 验证线程数
        if (config.getThreadCount() == null || config.getThreadCount() < 1 || config.getThreadCount() > 10) {
            throw MigrationConfigException.invalidThreadCount(config.getThreadCount() != null ? config.getThreadCount() : 0);
        }
        
        // 验证时间戳字段（增量同步时必需）
        if (Boolean.TRUE.equals(config.getIncrementalSync()) && !StringUtils.hasText(config.getTimestampField())) {
            throw MigrationConfigException.missingRequiredField("timestampField");
        }
        
        log.debug("迁移配置验证通过: sourceIndex={}, targetIndex={}, batchSize={}, threadCount={}", 
                 config.getSourceIndex(), config.getTargetIndex(), config.getBatchSize(), config.getThreadCount());
    }
    
    /**
     * 验证索引同步配置
     */
    public void validateIndexSyncConfig(IndexSyncConfig config) {
        if (config == null) {
            throw new MigrationConfigException("索引同步配置不能为空");
        }
        
        // 验证批次大小
        if (config.getBatchSize() == null || config.getBatchSize() < 1 || config.getBatchSize() > 10000) {
            throw MigrationConfigException.invalidBatchSize(config.getBatchSize() != null ? config.getBatchSize() : 0);
        }
        
        // 验证滚动超时时间
        if (config.getScrollTimeout() == null || config.getScrollTimeout() < 1 || config.getScrollTimeout() > 60) {
            throw MigrationConfigException.invalidScrollTimeout(config.getScrollTimeout() != null ? config.getScrollTimeout() : 0);
        }
        
        // 验证线程数
        if (config.getThreadCount() == null || config.getThreadCount() < 1 || config.getThreadCount() > 10) {
            throw MigrationConfigException.invalidThreadCount(config.getThreadCount() != null ? config.getThreadCount() : 0);
        }
        
        // 验证同步超时时间
        if (config.getSyncTimeoutMinutes() == null || config.getSyncTimeoutMinutes() < 1 || config.getSyncTimeoutMinutes() > 1440) { // 最大24小时
            throw new MigrationConfigException(
                String.format("同步超时时间无效: %d分钟, 必须在1-1440分钟之间", 
                             config.getSyncTimeoutMinutes() != null ? config.getSyncTimeoutMinutes() : 0),
                "syncTimeoutMinutes"
            );
        }
        
        // 验证时间戳字段（增量模式时必需）
        if (Boolean.TRUE.equals(config.getIncrementalMode()) && !StringUtils.hasText(config.getTimestampField())) {
            throw MigrationConfigException.missingRequiredField("timestampField");
        }
        
        // 至少要同步一种类型的内容
        if (!Boolean.TRUE.equals(config.getSyncSettings()) && 
            !Boolean.TRUE.equals(config.getSyncMappings()) && 
            !Boolean.TRUE.equals(config.getSyncAliases()) && 
            !Boolean.TRUE.equals(config.getSyncData())) {
            throw new MigrationConfigException("至少要启用一种同步类型：设置、映射、别名或数据");
        }
        
        log.debug("索引同步配置验证通过: batchSize={}, threadCount={}, syncTimeout={}分钟", 
                 config.getBatchSize(), config.getThreadCount(), config.getSyncTimeoutMinutes());
    }
    
    /**
     * 验证索引名称格式
     */
    public void validateIndexName(String indexName) {
        if (!StringUtils.hasText(indexName)) {
            throw new MigrationConfigException("索引名称不能为空");
        }
        
        // ES索引名称规则：小写字母、数字、-、_，不能以-、_、+开头
        if (!indexName.matches("^[a-z0-9]([a-z0-9_-]*[a-z0-9])?$")) {
            throw new MigrationConfigException(
                String.format("索引名称格式无效: %s，必须以小写字母或数字开头，只能包含小写字母、数字、-、_", indexName),
                "indexName"
            );
        }
        
        // 长度限制
        if (indexName.length() > 255) {
            throw new MigrationConfigException(
                String.format("索引名称过长: %d字符，最大允许255字符", indexName.length()),
                "indexName"
            );
        }
        
        // 禁止的索引名称
        String[] forbiddenNames = {".", "..", "_all"};
        for (String forbidden : forbiddenNames) {
            if (indexName.equals(forbidden)) {
                throw new MigrationConfigException(
                    String.format("不允许的索引名称: %s", indexName),
                    "indexName"
                );
            }
        }
    }
    
    /**
     * 验证批次大小是否合理
     */
    public void validateBatchSizeForDataSize(int batchSize, long documentCount) {
        if (documentCount > 1000000 && batchSize > 5000) {
            log.warn("大数据量索引({})建议使用较小的批次大小(当前: {})，可能会影响性能", 
                    documentCount, batchSize);
        }
        
        if (documentCount < 10000 && batchSize < 100) {
            log.info("小数据量索引({})可以考虑增大批次大小(当前: {})以提高效率", 
                    documentCount, batchSize);
        }
    }
    
    /**
     * 验证并自动优化配置
     */
    public MigrationConfig optimizeMigrationConfig(MigrationConfig config, long sourceDocumentCount) {
        // 验证基础配置
        validateMigrationConfig(config);
        
        // 根据数据量自动优化配置
        MigrationConfig optimizedConfig = new MigrationConfig();
        
        // 复制原始配置
        optimizedConfig.setSourceIndex(config.getSourceIndex());
        optimizedConfig.setTargetIndex(config.getTargetIndex());
        optimizedConfig.setFieldMapping(config.getFieldMapping());
        optimizedConfig.setNewFields(config.getNewFields());
        optimizedConfig.setCaseInsensitiveFields(config.getCaseInsensitiveFields());
        optimizedConfig.setIncrementalSync(config.getIncrementalSync());
        optimizedConfig.setTimestampField(config.getTimestampField());
        optimizedConfig.setDeleteNotExist(config.getDeleteNotExist());
        optimizedConfig.setQueryCondition(config.getQueryCondition());
        optimizedConfig.setExcludeFields(config.getExcludeFields());
        optimizedConfig.setIncludeFields(config.getIncludeFields());
        optimizedConfig.setOverwriteExisting(config.getOverwriteExisting());
        
        // 优化批次大小
        int optimizedBatchSize = config.getBatchSize();
        if (sourceDocumentCount > 10000000) {
            // 超大数据量，减小批次大小
            optimizedBatchSize = Math.min(optimizedBatchSize, 2000);
        } else if (sourceDocumentCount > 1000000) {
            // 大数据量，适度调整
            optimizedBatchSize = Math.min(optimizedBatchSize, 3000);
        } else if (sourceDocumentCount < 10000) {
            // 小数据量，可以增大批次大小
            optimizedBatchSize = Math.max(optimizedBatchSize, 500);
        }
        optimizedConfig.setBatchSize(optimizedBatchSize);
        
        // 优化线程数
        int optimizedThreadCount = config.getThreadCount();
        if (sourceDocumentCount > 5000000) {
            // 大数据量，适当增加线程数
            optimizedThreadCount = Math.min(optimizedThreadCount + 1, 5);
        } else if (sourceDocumentCount < 50000) {
            // 小数据量，减少线程数
            optimizedThreadCount = 1;
        }
        optimizedConfig.setThreadCount(optimizedThreadCount);
        
        // 优化滚动超时时间
        int optimizedScrollTimeout = config.getScrollTimeout();
        if (sourceDocumentCount > 5000000) {
            // 大数据量，增加超时时间
            optimizedScrollTimeout = Math.max(optimizedScrollTimeout, 10);
        }
        optimizedConfig.setScrollTimeout(optimizedScrollTimeout);
        
        if (optimizedBatchSize != config.getBatchSize() || 
            optimizedThreadCount != config.getThreadCount() || 
            optimizedScrollTimeout != config.getScrollTimeout()) {
            log.info("配置已优化 - 文档数量: {}, 批次大小: {} -> {}, 线程数: {} -> {}, 滚动超时: {} -> {}",
                    sourceDocumentCount,
                    config.getBatchSize(), optimizedBatchSize,
                    config.getThreadCount(), optimizedThreadCount,
                    config.getScrollTimeout(), optimizedScrollTimeout);
        }
        
        return optimizedConfig;
    }
}