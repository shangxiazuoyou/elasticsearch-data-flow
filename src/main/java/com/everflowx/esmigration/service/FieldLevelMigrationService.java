package com.everflowx.esmigration.service;

import com.everflowx.esmigration.checkpoint.*;
import com.everflowx.esmigration.domain.MigrationConfig;
import com.everflowx.esmigration.service.EsMigrationService;
import com.everflowx.esmigration.service.EsMigrationService.MigrationResult;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 字段级别迁移服务
 * 集成字段级别的细粒度断点续传功能
 * 
 * @author everflowx
 */
@Slf4j
@Service("fieldLevelMigrationService")
public class FieldLevelMigrationService {
    
    @Resource
    private AdvancedCheckpointManager checkpointManager;
    
    @Resource
    private FieldLevelRecoveryEngine recoveryEngine;
    
    @Resource
    @Qualifier("sourceElasticsearchClient")
    private RestHighLevelClient sourceClient;

    @Resource
    @Qualifier("targetElasticsearchClient")
    private RestHighLevelClient targetClient;
    
    @Resource
    @Qualifier("esMigrationServiceImpl")
    private EsMigrationService baseMigrationService;
    
    /**
     * 带字段级别断点续传的迁移
     */
    public MigrationResult fullMigrationWithFieldLevelRecovery(MigrationConfig config) {
        String taskId = generateTaskId(config);
        log.info("开始带字段级别恢复的全量迁移: taskId={}, 源索引={}, 目标索引={}", 
            taskId, config.getSourceIndex(), config.getTargetIndex());
        
        MigrationResult result = new MigrationResult();
        
        try {
            // 1. 检查是否有可恢复的断点
            ResumePlan resumePlan = checkpointManager.createResumePlan(taskId);
            
            if (resumePlan.getTotalCheckpoints() > 0) {
                log.info("发现可恢复的断点: {} 个，开始恢复", resumePlan.getTotalCheckpoints());
                
                // 执行断点恢复
                CompletableFuture<FieldLevelRecoveryEngine.RecoveryResult> recoveryFuture = 
                    recoveryEngine.executeRecoveryPlan(sourceClient, targetClient, config, resumePlan);
                
                FieldLevelRecoveryEngine.RecoveryResult recoveryResult = recoveryFuture.get();
                
                if (recoveryResult.isSuccess()) {
                    log.info("断点恢复完成: 成功恢复{}个断点", recoveryResult.getSuccessfulCheckpoints());
                    
                    // 更新迁移结果
                    result.setSuccessCount(result.getSuccessCount() + recoveryResult.getSuccessfulCheckpoints());
                    result.setSkippedCount(result.getSkippedCount() + recoveryResult.getTotalCheckpoints());
                    
                } else {
                    log.warn("断点恢复失败: {}", recoveryResult.getErrorMessage());
                }
            }
            
            // 2. 执行常规迁移（带增强的断点管理）
            executeEnhancedMigration(config, result, taskId);
            
        } catch (Exception e) {
            log.error("增强迁移执行失败", e);
            result.setErrorMessage("增强迁移失败: " + e.getMessage());
        }
        
        result.finish();
        
        // 输出详细的断点统计信息
        logCheckpointStatistics(taskId);
        
        log.info("带字段级别恢复的迁移完成: taskId={}, 成功={}, 失败={}, 跳过={}", 
            taskId, result.getSuccessCount(), result.getFailedCount(), result.getSkippedCount());
        
        return result;
    }
    
    /**
     * 执行增强的迁移（带细粒度断点）
     */
    private void executeEnhancedMigration(MigrationConfig config, MigrationResult result, String taskId) {
        try {
            // 获取总文档数
            long totalCount = baseMigrationService.getDocumentCount(config.getSourceIndex(), false);
            result.setTotalCount(totalCount);
            
            // 执行增强的单线程迁移
            if (config.getThreadCount() <= 1) {
                executeEnhancedSingleThreadMigration(config, result, taskId);
            } else {
                executeEnhancedParallelMigration(config, result, taskId);
            }
            
        } catch (Exception e) {
            log.error("增强迁移执行异常", e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 增强的单线程迁移
     */
    private void executeEnhancedSingleThreadMigration(MigrationConfig config, MigrationResult result, String taskId) {
        log.info("开始增强的单线程迁移: taskId={}", taskId);
        
        AtomicLong batchNumber = new AtomicLong(1);
        
        try {
            // 这里重用父类的滚动查询逻辑，但在批次处理时增加断点管理
            org.elasticsearch.action.search.SearchRequest searchRequest = 
                new org.elasticsearch.action.search.SearchRequest(config.getSourceIndex());
            org.elasticsearch.search.builder.SearchSourceBuilder searchSourceBuilder = 
                new org.elasticsearch.search.builder.SearchSourceBuilder();
            searchSourceBuilder.query(org.elasticsearch.index.query.QueryBuilders.matchAllQuery());
            searchSourceBuilder.size(config.getBatchSize());
            searchSourceBuilder.trackTotalHits(true);
            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(org.elasticsearch.common.unit.TimeValue.timeValueMinutes(config.getScrollTimeout()));
            
            org.elasticsearch.action.search.SearchResponse searchResponse = 
                sourceClient.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            
            while (searchHits != null && searchHits.length > 0) {
                // 创建批次级别断点
                List<String> batchDocIds = Arrays.stream(searchHits)
                    .map(SearchHit::getId)
                    .collect(Collectors.toList());
                
                FieldLevelCheckpoint batchCheckpoint = checkpointManager.createBatchCheckpoint(
                    taskId, batchNumber.get(), batchDocIds, searchHits.length);
                
                try {
                    // 处理批次（带字段级别断点）
                    processBatchWithFieldLevelCheckpoints(searchHits, config, result, taskId, batchCheckpoint);
                    
                    // 标记批次完成
                    checkpointManager.updateBatchProgress(batchCheckpoint.getCheckpointId(), 
                        searchHits.length, result.getSuccessCount(), result.getFailedCount());
                    
                } catch (Exception e) {
                    log.error("批次处理失败: batchNumber=" + batchNumber.get(), e);
                    checkpointManager.markDocumentFailed(taskId, "batch_" + batchNumber.get(), e.getMessage());
                }
                
                batchNumber.incrementAndGet();
                
                // 继续滚动
                org.elasticsearch.action.search.SearchScrollRequest scrollRequest = 
                    new org.elasticsearch.action.search.SearchScrollRequest(scrollId);
                scrollRequest.scroll(org.elasticsearch.common.unit.TimeValue.timeValueMinutes(config.getScrollTimeout()));
                
                searchResponse = sourceClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }
            
        } catch (Exception e) {
            log.error("增强单线程迁移异常", e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 增强的并行迁移
     */
    private void executeEnhancedParallelMigration(MigrationConfig config, MigrationResult result, String taskId) {
        log.info("开始增强的并行迁移: taskId={}, 线程数={}", taskId, config.getThreadCount());
        
        // 为简化实现，这里先调用原有的并行迁移，后续可以进一步增强
        try {
            // 暂时使用简化的并行迁移实现
            log.warn("并行迁移暂未实现字段级别断点，使用单线程模式");
            executeEnhancedSingleThreadMigration(config, result, taskId);
        } catch (Exception e) {
            log.error("增强并行迁移异常", e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 带字段级别断点的批次处理
     */
    private void processBatchWithFieldLevelCheckpoints(
            SearchHit[] hits, 
            MigrationConfig config, 
            MigrationResult result, 
            String taskId,
            FieldLevelCheckpoint batchCheckpoint) {
        
        BulkRequest bulkRequest = new BulkRequest();
        Map<String, FieldLevelCheckpoint> docFieldCheckpoints = new HashMap<>();
        
        // 为每个文档创建字段级别断点
        for (SearchHit hit : hits) {
            try {
                Map<String, Object> sourceData = hit.getSourceAsMap();
                
                // 为关键字段创建断点
                for (String fieldName : getKeyFields(config, sourceData)) {
                    Object fieldValue = sourceData.get(fieldName);
                    if (fieldValue != null) {
                        FieldLevelCheckpoint fieldCheckpoint = checkpointManager.createFieldCheckpoint(
                            taskId, hit.getId(), fieldName, fieldValue);
                        docFieldCheckpoints.put(hit.getId() + ":" + fieldName, fieldCheckpoint);
                    }
                }
                
                // 转换文档
                Map<String, Object> targetData = transformDocumentData(sourceData, config);
                
                IndexRequest indexRequest = new IndexRequest(config.getTargetIndex())
                    .id(hit.getId())
                    .source(targetData, XContentType.JSON);
                
                bulkRequest.add(indexRequest);
                
            } catch (Exception e) {
                log.error("文档预处理失败: docId=" + hit.getId(), e);
                result.setFailedCount(result.getFailedCount() + 1);
                
                // 创建文档级别的失败断点
                checkpointManager.markDocumentFailed(taskId, hit.getId(), e.getMessage());
            }
        }
        
        // 执行批量写入
        if (bulkRequest.numberOfActions() > 0) {
            try {
                BulkResponse bulkResponse = targetClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                
                // 详细处理每个文档的结果
                processDetailedBulkResponse(bulkResponse, docFieldCheckpoints, result, taskId);
                
            } catch (Exception e) {
                log.error("批量写入失败", e);
                result.setFailedCount(result.getFailedCount() + bulkRequest.numberOfActions());
                
                // 为所有文档创建失败断点
                for (int i = 0; i < bulkRequest.numberOfActions(); i++) {
                    IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(i);
                    checkpointManager.markDocumentFailed(taskId, indexRequest.id(), e.getMessage());
                }
            }
        }
    }
    
    /**
     * 详细处理批量响应
     */
    private void processDetailedBulkResponse(
            BulkResponse bulkResponse,
            Map<String, FieldLevelCheckpoint> docFieldCheckpoints,
            MigrationResult result,
            String taskId) {
        
        for (BulkItemResponse item : bulkResponse.getItems()) {
            if (item.isFailed()) {
                result.setFailedCount(result.getFailedCount() + 1);
                
                // 标记文档失败
                checkpointManager.markDocumentFailed(taskId, item.getId(), item.getFailureMessage());
                
                // 标记相关字段检查点失败
                docFieldCheckpoints.entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(item.getId() + ":"))
                    .forEach(entry -> {
                        FieldLevelCheckpoint checkpoint = entry.getValue();
                        String fieldName = entry.getKey().substring(entry.getKey().indexOf(":") + 1);
                        checkpointManager.updateFieldCheckpoint(
                            checkpoint.getCheckpointId(),
                            fieldName,
                            null,
                            FieldLevelCheckpoint.FieldCheckpoint.FieldProcessStatus.FAILED
                        );
                    });
                    
                log.debug("文档写入失败: docId={}, 原因={}", item.getId(), item.getFailureMessage());
                
            } else {
                result.setSuccessCount(result.getSuccessCount() + 1);
                
                // 标记相关字段检查点成功
                docFieldCheckpoints.entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(item.getId() + ":"))
                    .forEach(entry -> {
                        FieldLevelCheckpoint checkpoint = entry.getValue();
                        String fieldName = entry.getKey().substring(entry.getKey().indexOf(":") + 1);
                        checkpointManager.updateFieldCheckpoint(
                            checkpoint.getCheckpointId(),
                            fieldName,
                            null, // transformedValue 可以从 targetData 中获取
                            FieldLevelCheckpoint.FieldCheckpoint.FieldProcessStatus.INDEXED
                        );
                    });
            }
        }
    }
    
    /**
     * 转换文档数据
     */
    private Map<String, Object> transformDocumentData(Map<String, Object> sourceData, MigrationConfig config) {
        Map<String, Object> targetData = new HashMap<>();
        
        // 字段映射
        if (config.getFieldMapping() != null) {
            for (Map.Entry<String, String> mapping : config.getFieldMapping().entrySet()) {
                String sourceField = mapping.getKey();
                String targetField = mapping.getValue();
                if (sourceData.containsKey(sourceField)) {
                    Object value = sourceData.get(sourceField);
                    // 处理忽略大小写的字段
                    if (config.getCaseInsensitiveFields() != null && 
                        config.getCaseInsensitiveFields().contains(targetField) && 
                        value instanceof String) {
                        value = ((String) value).toLowerCase();
                    }
                    targetData.put(targetField, value);
                }
            }
        } else {
            // 如果没有字段映射，直接复制所有字段
            targetData.putAll(sourceData);
        }
        
        // 添加新字段
        if (config.getNewFields() != null) {
            targetData.putAll(config.getNewFields());
        }
        
        // 处理忽略大小写的字段
        if (config.getCaseInsensitiveFields() != null) {
            for (String field : config.getCaseInsensitiveFields()) {
                if (targetData.containsKey(field) && targetData.get(field) instanceof String) {
                    targetData.put(field, ((String) targetData.get(field)).toLowerCase());
                }
            }
        }
        
        // 排除字段
        if (config.getExcludeFields() != null) {
            for (String excludeField : config.getExcludeFields()) {
                targetData.remove(excludeField);
            }
        }
        
        // 只包含指定字段（只有当includeFields不为空且包含字段时才过滤）
        if (config.getIncludeFields() != null && !config.getIncludeFields().isEmpty()) {
            Map<String, Object> filteredData = new HashMap<>();
            for (String includeField : config.getIncludeFields()) {
                if (targetData.containsKey(includeField)) {
                    filteredData.put(includeField, targetData.get(includeField));
                }
            }
            // 只有当过滤后还有数据时才替换，避免空数组导致所有字段被过滤
            if (!filteredData.isEmpty()) {
                targetData = filteredData;
            }
        }
        
        return targetData;
    }
    
    /**
     * 获取关键字段列表
     */
    private Set<String> getKeyFields(MigrationConfig config, Map<String, Object> sourceData) {
        Set<String> keyFields = new HashSet<>();
        
        // 添加配置中的关键字段
        if (config.getFieldMapping() != null) {
            keyFields.addAll(config.getFieldMapping().keySet());
        }
        
        if (config.getCaseInsensitiveFields() != null) {
            keyFields.addAll(config.getCaseInsensitiveFields());
        }
        
        // 添加时间戳字段
        if (config.getTimestampField() != null) {
            keyFields.add(config.getTimestampField());
        }
        
        // 如果没有配置关键字段，选择一些常见的重要字段
        if (keyFields.isEmpty()) {
            for (String fieldName : sourceData.keySet()) {
                if (fieldName.contains("id") || fieldName.contains("name") || 
                    fieldName.contains("time") || fieldName.contains("status")) {
                    keyFields.add(fieldName);
                    if (keyFields.size() >= 5) break; // 限制字段数量
                }
            }
        }
        
        return keyFields;
    }
    
    /**
     * 记录断点统计信息
     */
    private void logCheckpointStatistics(String taskId) {
        try {
            AdvancedCheckpointManager.CheckpointStatistics stats = checkpointManager.getStatistics(taskId);
            
            log.info("=== 断点统计信息 ===");
            log.info("任务ID: {}", stats.getTaskId());
            log.info("总断点数: {}", stats.getTotalCheckpoints());
            log.info("待处理: {}", stats.getPendingCount());
            log.info("处理中: {}", stats.getProcessingCount());
            log.info("已完成: {}", stats.getCompletedCount());
            log.info("失败: {}", stats.getFailedCount());
            log.info("暂停: {}", stats.getSuspendedCount());
            log.info("成功率: {:.2f}%", stats.getSuccessRate());
            log.info("失败率: {:.2f}%", stats.getFailureRate());
            log.info("--- 按类型分布 ---");
            log.info("文档级别: {}", stats.getDocumentLevelCount());
            log.info("字段级别: {}", stats.getFieldLevelCount());
            log.info("批次级别: {}", stats.getBatchLevelCount());
            log.info("时间范围级别: {}", stats.getTimeRangeLevelCount());
            log.info("==================");
            
        } catch (Exception e) {
            log.warn("记录断点统计信息失败", e);
        }
    }
    
    /**
     * 获取失败文档的详细信息
     */
    public List<String> getFailedDocuments(String taskId) {
        Set<String> failedDocs = checkpointManager.getFailedDocuments(taskId);
        return new ArrayList<>(failedDocs);
    }
    
    /**
     * 手动触发断点恢复
     */
    public CompletableFuture<FieldLevelRecoveryEngine.RecoveryResult> manualRecovery(
            String taskId, MigrationConfig config) {
        
        log.info("手动触发断点恢复: taskId={}", taskId);
        
        ResumePlan resumePlan = checkpointManager.createResumePlan(taskId);
        
        if (resumePlan.getTotalCheckpoints() == 0) {
            log.info("没有可恢复的断点: taskId={}", taskId);
            
            FieldLevelRecoveryEngine.RecoveryResult emptyResult = new FieldLevelRecoveryEngine.RecoveryResult();
            emptyResult.setTaskId(taskId);
            emptyResult.setSuccess(true);
            emptyResult.setStartTime(new Date());
            emptyResult.setEndTime(new Date());
            
            return CompletableFuture.completedFuture(emptyResult);
        }
        
        log.info("开始手动恢复: 断点数={}", resumePlan.getTotalCheckpoints());
        
        return recoveryEngine.executeRecoveryPlan(sourceClient, targetClient, config, resumePlan);
    }
    
    /**
     * 清理过期断点
     */
    public void cleanupExpiredCheckpoints() {
        log.info("开始清理过期断点");
        checkpointManager.cleanupExpiredCheckpoints();
        log.info("过期断点清理完成");
    }
    
    /**
     * 生成任务ID
     */
    private String generateTaskId(MigrationConfig config) {
        return String.format("field_level_migration_%s_%s_%d", 
            config.getSourceIndex(), 
            config.getTargetIndex(), 
            System.currentTimeMillis());
    }
}