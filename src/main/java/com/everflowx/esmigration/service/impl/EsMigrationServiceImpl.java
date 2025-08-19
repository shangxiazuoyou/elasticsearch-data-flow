package com.everflowx.esmigration.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.everflowx.esmigration.config.ElasticsearchConfig;
import com.everflowx.esmigration.domain.MigrationConfig;
import com.everflowx.esmigration.service.EsMigrationService;
import com.everflowx.esmigration.util.EsQueryHelper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import com.everflowx.esmigration.service.CheckpointService;
import com.everflowx.esmigration.domain.MigrationCheckpoint;
import com.everflowx.esmigration.monitor.MigrationMonitor;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ES数据迁移服务实现类
 * 
 * @author everflowx
 */
@Slf4j
@Service
public class EsMigrationServiceImpl implements EsMigrationService {
    
    @Resource
    @Qualifier("sourceElasticsearchClient")
    private RestHighLevelClient sourceClient;

    @Resource
    @Qualifier("targetElasticsearchClient")
    private RestHighLevelClient targetClient;

    @Resource
    private ElasticsearchConfig elasticsearchConfig;

    @Resource
    private EsQueryHelper esQueryHelper;
    
    @Resource
    private CheckpointService checkpointService;
    
    @Resource
    private MigrationMonitor migrationMonitor;
    
    private static final String SCROLL_ID_TIMEOUT = "5m";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    
    @Override
    public MigrationResult fullMigration(MigrationConfig config) {
        String taskId = generateTaskId(config);
        log.info("开始全量数据迁移，任务ID: {}, 源索引: {}, 目标索引: {}, 响应缓冲区: {}MB",
                taskId, config.getSourceIndex(), config.getTargetIndex(),
                elasticsearchConfig.getResponseBufferLimit() / 1024 / 1024);
        log.info("{}", elasticsearchConfig.getConnectionPoolInfo());
        log.info("{}", elasticsearchConfig.getTimeoutInfo());
        
        MigrationResult result = new MigrationResult();
        
        // 检查是否可以从断点恢复
        Optional<MigrationCheckpoint> checkpointOpt = Optional.empty();
        if (checkpointService.canResumeFromCheckpoint(taskId, config)) {
            checkpointOpt = checkpointService.getCheckpoint(taskId);
            if (checkpointOpt.isPresent()) {
                MigrationCheckpoint checkpoint = checkpointOpt.get();
                log.info("检测到可恢复的断点，已处理: {}, 成功: {}, 失败: {}", 
                    checkpoint.getProcessedCount(), checkpoint.getSuccessCount(), checkpoint.getFailedCount());
                
                result.setSuccessCount(checkpoint.getSuccessCount());
                result.setFailedCount(checkpoint.getFailedCount());
            }
        }
        
        try {
            // 检查源索引是否存在
            if (!indexExists(config.getSourceIndex(), false)) {
                result.setErrorMessage("源索引不存在: " + config.getSourceIndex());
                result.finish();
                return result;
            }
            
            // 检查或创建目标索引
            if (!indexExists(config.getTargetIndex(), true)) {
                String sourceMapping = getIndexMapping(config.getSourceIndex(), false);
                String targetMapping = adaptMapping(sourceMapping, config);
                if (!createIndex(config.getTargetIndex(), targetMapping, true)) {
                    result.setErrorMessage("创建目标索引失败: " + config.getTargetIndex());
                    result.finish();
                    return result;
                }
            }
            
            // 获取总文档数
            long totalCount = getDocumentCount(config.getSourceIndex(), false);
            result.setTotalCount(totalCount);
            log.info("源索引文档总数: {}", totalCount);
            
            // 开始监控任务
            migrationMonitor.startTask(taskId, config.getSourceIndex(), config.getTargetIndex(), totalCount);
            
            // 执行迁移
            if (config.getThreadCount() > 1) {
                executeParallelMigrationWithCheckpoint(config, result, taskId, checkpointOpt);
            } else {
                executeSingleThreadMigrationWithCheckpoint(config, result, taskId, checkpointOpt);
            }
            
        } catch (Exception e) {
            log.error("全量迁移失败", e);
            result.setErrorMessage("迁移失败: " + e.getMessage());
        }
        
        result.finish();
        
        // 清理断点信息
        if (result.getErrorMessage() == null) {
            checkpointService.removeCheckpoint(taskId);
            log.info("迁移成功完成，清理断点信息");
        } else {
            // 保存失败状态
            MigrationCheckpoint failedCheckpoint = new MigrationCheckpoint();
            failedCheckpoint.setTaskId(taskId);
            failedCheckpoint.setSourceIndex(config.getSourceIndex());
            failedCheckpoint.setTargetIndex(config.getTargetIndex());
            failedCheckpoint.setStatus("FAILED");
            failedCheckpoint.setErrorMessage(result.getErrorMessage());
            failedCheckpoint.setSuccessCount(result.getSuccessCount());
            failedCheckpoint.setFailedCount(result.getFailedCount());
            failedCheckpoint.setTotalCount(result.getTotalCount());
            checkpointService.saveCheckpoint(failedCheckpoint);
        }
        
        // 完成任务监控
        String finalStatus = result.getErrorMessage() == null ? "COMPLETED" : "FAILED";
        migrationMonitor.completeTask(taskId, finalStatus);
        migrationMonitor.logDetailedMetrics(taskId);
        migrationMonitor.logSystemMetrics();
        
        log.info("全量迁移完成，任务ID: {}, 总数: {}, 成功: {}, 失败: {}, 跳过: {}, 耗时: {}ms", 
                taskId, result.getTotalCount(), result.getSuccessCount(), 
                result.getFailedCount(), result.getSkippedCount(), result.getDurationMs());
        
        return result;
    }
    
    @Override
    public MigrationResult incrementalMigration(MigrationConfig config, Date lastSyncTime) {
        log.info("开始增量数据迁移，源索引: {}, 目标索引: {}, 上次同步时间: {}, 响应缓冲区: {}MB",
                config.getSourceIndex(), config.getTargetIndex(), lastSyncTime,
                elasticsearchConfig.getResponseBufferLimit() / 1024 / 1024);
        
        MigrationResult result = new MigrationResult();
        
        try {
            // 构建增量查询条件
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
            
            if (lastSyncTime != null) {
                RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(config.getTimestampField())
                        .gte(DATE_FORMAT.format(lastSyncTime));
                queryBuilder.must(rangeQuery);
            }
            
            // 添加自定义查询条件
            if (config.getQueryCondition() != null && !config.getQueryCondition().isEmpty()) {
                // 这里可以解析JSON查询条件并添加到queryBuilder中
                // 为简化示例，暂时跳过复杂查询解析
            }
            
            // 执行增量迁移
            executeIncrementalMigration(config, queryBuilder, result);
            
        } catch (Exception e) {
            log.error("增量迁移失败", e);
            result.setErrorMessage("增量迁移失败: " + e.getMessage());
        }
        
        result.finish();
        log.info("增量迁移完成，总数: {}, 成功: {}, 失败: {}, 跳过: {}, 耗时: {}ms", 
                result.getTotalCount(), result.getSuccessCount(), 
                result.getFailedCount(), result.getSkippedCount(), result.getDurationMs());
        
        return result;
    }
    
    /**
     * 执行单线程迁移 - 增加内存管理和动态调整
     */
    private void executeSingleThreadMigration(MigrationConfig config, MigrationResult result) throws IOException {
        SearchRequest searchRequest = new SearchRequest(config.getSourceIndex());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        
        // 动态批次大小管理
        int currentBatchSize = config.getBatchSize();
        searchSourceBuilder.size(currentBatchSize);
        searchSourceBuilder.trackTotalHits(true);
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(config.getScrollTimeout()));

        log.info("开始执行单线程全量迁移，初始批次大小: {}, {}", currentBatchSize, esQueryHelper.getBufferConfigInfo());
        SearchResponse searchResponse = esQueryHelper.searchWithRetry(sourceClient, searchRequest, true, 3);
        String scrollId = searchResponse.getScrollId();

        SearchHit[] searchHits = searchResponse.getHits().getHits();
        long totalProcessed = 0;
        long startTime = System.currentTimeMillis();
        long lastGcTime = getGcTime();
        int consecutiveErrors = 0;

        while (searchHits != null && searchHits.length > 0) {
            long batchStartTime = System.currentTimeMillis();
            long beforeBatchSuccess = result.getSuccessCount();
            
            // 检查内存情况并调整批次大小
            int newBatchSize = adjustBatchSizeBasedOnMemory(currentBatchSize, totalProcessed, result.getTotalCount());
            if (newBatchSize != currentBatchSize) {
                currentBatchSize = newBatchSize;
                log.info("动态调整批次大小: {} -> {}", config.getBatchSize(), currentBatchSize);
            }
            
            try {
                processBatch(searchHits, config, result);
                consecutiveErrors = 0; // 成功处理，重置错误计数
                
                // 计算处理速度并更新监控
                long batchTime = System.currentTimeMillis() - batchStartTime;
                long batchSuccess = result.getSuccessCount() - beforeBatchSuccess;
                if (batchTime > 0) {
                    double speed = (double) batchSuccess / batchTime * 1000; // docs/sec
                    
                    // 更新监控数据
                    migrationMonitor.updateProgress(taskId, totalProcessed, result.getSuccessCount(), result.getFailedCount());
                    
                    if (totalProcessed % (currentBatchSize * 5) == 0) {
                        logPerformanceMetrics(totalProcessed, result, startTime, speed, currentBatchSize);
                        migrationMonitor.logDetailedMetrics(taskId);
                    }
                }
                
            } catch (Exception e) {
                consecutiveErrors++;
                log.error("批次处理失败 ({} 次连续错误): {}", consecutiveErrors, e.getMessage());
                
                if (consecutiveErrors >= 3) {
                    log.error("连续错误过多，降低批次大小并继续");
                    currentBatchSize = Math.max(currentBatchSize / 2, 10);
                    consecutiveErrors = 0;
                }
            }
            
            totalProcessed += searchHits.length;
            
            // 内存清理
            if (totalProcessed % (currentBatchSize * 10) == 0) {
                performMemoryCleanup(lastGcTime);
                lastGcTime = getGcTime();
            }

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueMinutes(config.getScrollTimeout()));

            try {
                searchResponse = esQueryHelper.scrollWithRetry(sourceClient, scrollRequest, true, 3);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            } catch (IOException e) {
                log.error("滚动查询重试失败，停止迁移: {}", e.getMessage());
                throw e;
            }
        }
    }
    
    /**
     * 根据内存使用情况调整批次大小
     */
    private int adjustBatchSizeBasedOnMemory(int currentBatchSize, long processedCount, long totalCount) {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        double memoryUsagePercent = (double) usedMemory / totalMemory * 100;
        
        // 计算进度百分比
        double progress = totalCount > 0 ? (double) processedCount / totalCount : 0;
        
        int newBatchSize = currentBatchSize;
        
        if (memoryUsagePercent > 85) {
            // 内存使用过高，减小批次大小
            newBatchSize = Math.max(currentBatchSize / 2, 50);
            log.warn("内存使用过高 {:.1f}%，减小批次大小: {} -> {}", 
                memoryUsagePercent, currentBatchSize, newBatchSize);
        } else if (memoryUsagePercent < 50 && progress < 0.8) {
            // 内存充足且进度较少，可以适当增加批次大小
            newBatchSize = Math.min(currentBatchSize * 2, 5000);
            if (newBatchSize != currentBatchSize) {
                log.info("内存充足 {:.1f}%，增加批次大小: {} -> {}", 
                    memoryUsagePercent, currentBatchSize, newBatchSize);
            }
        }
        
        return newBatchSize;
    }
    
    /**
     * 获取GC时间
     */
    private long getGcTime() {
        try {
            return java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()
                .stream()
                .mapToLong(gcBean -> gcBean.getCollectionTime())
                .sum();
        } catch (Exception e) {
            return 0;
        }
    }
    
    /**
     * 执行内存清理
     */
    private void performMemoryCleanup(long lastGcTime) {
        long currentGcTime = getGcTime();
        long gcTimeDiff = currentGcTime - lastGcTime;
        
        if (gcTimeDiff > 1000) { // GC时间超过1秒
            log.warn("检测到频繁GC，GC耗时: {}ms，建议手动清理内存", gcTimeDiff);
        }
        
        Runtime runtime = Runtime.getRuntime();
        long beforeGc = runtime.freeMemory();
        System.gc(); // 手动触发GC
        long afterGc = runtime.freeMemory();
        
        if (afterGc > beforeGc) {
            log.debug("内存清理效果: 释放 {}MB", (afterGc - beforeGc) / 1024 / 1024);
        }
    }
    
    /**
     * 记录性能指标
     */
    private void logPerformanceMetrics(long processedCount, MigrationResult result, long startTime, double currentSpeed, int batchSize) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        double avgSpeed = processedCount > 0 ? (double) processedCount / elapsedTime * 1000 : 0;
        
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;
        long maxMemory = runtime.maxMemory() / 1024 / 1024;
        double memoryUsage = (double) usedMemory / maxMemory * 100;
        
        long remaining = result.getTotalCount() - processedCount;
        long eta = avgSpeed > 0 ? (long)(remaining / avgSpeed) : -1;
        
        log.info("性能指标 - 已处理: {}, 当前速度: {:.1f} docs/s, 平均速度: {:.1f} docs/s, " +
                 "内存使用: {}MB/{} MB ({:.1f}%), 批次大小: {}, 预计剩余: {}s", 
                 processedCount, currentSpeed, avgSpeed, usedMemory, maxMemory, memoryUsage, batchSize, 
                 eta > 0 ? eta : "unknown");
    }
    
    /**
     * 处理批次数据 - 修复错误计数逻辑
     */
    private void processBatch(SearchHit[] hits, MigrationConfig config, MigrationResult result) {
        BulkRequest bulkRequest = new BulkRequest();
        AtomicLong batchPreprocessFailed = new AtomicLong(0);
        
        for (SearchHit hit : hits) {
            try {
                Map<String, Object> sourceData = hit.getSourceAsMap();
                Map<String, Object> targetData = transformDocument(sourceData, config);
                
                IndexRequest indexRequest = new IndexRequest(config.getTargetIndex())
                        .id(hit.getId())
                        .source(targetData, XContentType.JSON);
                
                bulkRequest.add(indexRequest);
                
            } catch (Exception e) {
                log.error("预处理文档失败，ID: {}, 错误: {}", hit.getId(), e.getMessage());
                batchPreprocessFailed.incrementAndGet();
            }
        }
        
        // 更新预处理失败数
        result.setFailedCount(result.getFailedCount() + batchPreprocessFailed.get());
        
        if (bulkRequest.numberOfActions() > 0) {
            try {
                BulkResponse bulkResponse = targetClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                
                // 修复：正确统计成功和失败数量
                AtomicLong batchSuccessCount = new AtomicLong(0);
                AtomicLong batchFailedCount = new AtomicLong(0);
                
                if (bulkResponse.hasFailures()) {
                    // 逐个检查每个操作的结果
                    Arrays.stream(bulkResponse.getItems())
                        .forEach(item -> {
                            if (item.isFailed()) {
                                batchFailedCount.incrementAndGet();
                                log.debug("文档写入失败: {}, 原因: {}", item.getId(), item.getFailureMessage());
                            } else {
                                batchSuccessCount.incrementAndGet();
                            }
                        });
                    
                    log.warn("批量写入部分失败: 成功 {} 件，失败 {} 件", batchSuccessCount.get(), batchFailedCount.get());
                } else {
                    // 全部成功
                    batchSuccessCount.set(bulkResponse.getItems().length);
                }
                
                // 更新结果计数
                long oldSuccessCount = result.getSuccessCount();
                long oldFailedCount = result.getFailedCount();
                result.setSuccessCount(result.getSuccessCount() + batchSuccessCount.get());
                result.setFailedCount(result.getFailedCount() + batchFailedCount.get());
                
                // 记录详细进度并更新监控
                if (result.getSuccessCount() % 10000 == 0 || 
                    (batchSuccessCount.get() > 0 && result.getSuccessCount() != oldSuccessCount)) {
                    long totalProcessed = result.getSuccessCount() + result.getFailedCount();
                    double successRate = totalProcessed > 0 ? (double) result.getSuccessCount() / totalProcessed * 100 : 0;
                    log.info("进度更新: 成功 {} 件，失败 {} 件，成功率 {:.2f}%", 
                        result.getSuccessCount(), result.getFailedCount(), successRate);
                    
                    // 更新监控数据
                    migrationMonitor.updateProgress(taskId, totalProcessed, result.getSuccessCount(), result.getFailedCount());
                }
                
            } catch (IOException e) {
                log.error("批量写入完全失败", e);
                result.setFailedCount(result.getFailedCount() + bulkRequest.numberOfActions());
            }
        }
    }
    
    /**
     * 转换文档数据
     */
    private Map<String, Object> transformDocument(Map<String, Object> sourceData, MigrationConfig config) {
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
     * 执行并行迁移
     */
    private void executeParallelMigration(MigrationConfig config, MigrationResult result) throws IOException {
        int threadCount = config.getThreadCount();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<CompletableFuture<MigrationResult>> futures = new ArrayList<>();
        
        log.info("开始执行并行迁移，线程数: {}", threadCount);
        
        // 获取总文档数进行分片
        long totalCount = getDocumentCount(config.getSourceIndex(), false);
        long docsPerThread = Math.max(totalCount / threadCount, 1000);
        
        for (int i = 0; i < threadCount; i++) {
            final int shardIndex = i;
            final long startOffset = i * docsPerThread;
            final long endOffset = (i == threadCount - 1) ? totalCount : (i + 1) * docsPerThread;
            
            CompletableFuture<MigrationResult> future = CompletableFuture.supplyAsync(() -> {
                try {
                    return executeShardMigration(config, shardIndex, startOffset, endOffset);
                } catch (Exception e) {
                    log.error("分片 {} 迁移失败", shardIndex, e);
                    MigrationResult shardResult = new MigrationResult();
                    shardResult.setErrorMessage("分片迁移失败: " + e.getMessage());
                    return shardResult;
                }
            }, executor);
            futures.add(future);
        }
        
        // 等待所有分片完成并合并结果
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        
        for (CompletableFuture<MigrationResult> future : futures) {
            try {
                MigrationResult shardResult = future.get();
                result.setSuccessCount(result.getSuccessCount() + shardResult.getSuccessCount());
                result.setFailedCount(result.getFailedCount() + shardResult.getFailedCount());
                result.setSkippedCount(result.getSkippedCount() + shardResult.getSkippedCount());
                if (shardResult.getErrorMessage() != null) {
                    String currentError = result.getErrorMessage();
                    result.setErrorMessage(currentError == null ? shardResult.getErrorMessage() : 
                        currentError + "; " + shardResult.getErrorMessage());
                }
            } catch (Exception e) {
                log.error("获取分片结果失败", e);
                result.setErrorMessage("分片结果合并失败: " + e.getMessage());
            }
        }
        
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.HOURS)) {
                log.warn("并行迁移超时，强制关闭线程池");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("并行迁移被中断", e);
            executor.shutdownNow();
        }
    }

    /**
     * 执行增量迁移
     */
    private void executeIncrementalMigration(MigrationConfig config, BoolQueryBuilder queryBuilder, MigrationResult result) throws IOException {
        SearchRequest searchRequest = new SearchRequest(config.getSourceIndex());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.size(config.getBatchSize());
        searchSourceBuilder.sort(config.getTimestampField(), SortOrder.ASC);
        // 确保能获取准确的文档总数
        searchSourceBuilder.trackTotalHits(true);
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(config.getScrollTimeout()));

        SearchResponse searchResponse = sourceClient.search(searchRequest, elasticsearchConfig.getCustomRequestOptions());
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        result.setTotalCount(searchResponse.getHits().getTotalHits().value);

        while (searchHits != null && searchHits.length > 0) {
            processBatch(searchHits, config, result);

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueMinutes(config.getScrollTimeout()));
            searchResponse = sourceClient.scroll(scrollRequest, elasticsearchConfig.getCustomRequestOptions());
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        }
    }

    @Override
    public boolean indexExists(String indexName, boolean isTarget) {
        try {
            RestHighLevelClient client = isTarget ? targetClient : sourceClient;
            GetIndexRequest request = new GetIndexRequest(indexName);
            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("检查索引是否存在失败: {}", indexName, e);
            return false;
        }
    }

    @Override
    public boolean createIndex(String indexName, String mapping, boolean isTarget) {
        try {
            RestHighLevelClient client = isTarget ? targetClient : sourceClient;
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            if (mapping != null && !mapping.isEmpty()) {
                request.source(mapping, XContentType.JSON);
            }
            client.indices().create(request, RequestOptions.DEFAULT);
            log.info("成功创建索引: {}", indexName);
            return true;
        } catch (IOException e) {
            log.error("创建索引失败: {}", indexName, e);
            return false;
        }
    }

    @Override
    public String getIndexMapping(String indexName, boolean isTarget) {
        try {
            RestHighLevelClient client = isTarget ? targetClient : sourceClient;
            GetMappingsRequest request = new GetMappingsRequest();
            request.indices(indexName);
            GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
            return response.toString();
        } catch (IOException e) {
            log.error("获取索引映射失败: {}", indexName, e);
            return null;
        }
    }

    @Override
    public long getDocumentCount(String indexName, boolean isTarget) {
        try {
            RestHighLevelClient client = isTarget ? targetClient : sourceClient;
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.size(0);
            // 关键修复：设置trackTotalHits为true以获取准确的文档总数
            searchSourceBuilder.trackTotalHits(true);
            searchRequest.source(searchSourceBuilder);

            log.debug("获取索引 {} 的文档数量", indexName);
            SearchResponse searchResponse = client.search(searchRequest, elasticsearchConfig.getCustomRequestOptions());

            long totalHits = searchResponse.getHits().getTotalHits().value;
            log.info("索引 {} 的文档总数: {}", indexName, totalHits);

            return totalHits;
        } catch (IOException e) {
            log.error("获取文档数量失败: {}", indexName, e);
            return 0;
        }
    }

    @Override
    public ValidationResult validateMigration(MigrationConfig config) {
        ValidationResult result = new ValidationResult();

        try {
            log.info("开始验证迁移结果，源索引: {}, 目标索引: {}", config.getSourceIndex(), config.getTargetIndex());

            // 使用智能计数方法获取准确的文档数量
            long sourceCount = getSmartDocumentCount(config.getSourceIndex(), false);
            long targetCount = getSmartDocumentCount(config.getTargetIndex(), true);

            result.setSourceCount(sourceCount);
            result.setTargetCount(targetCount);

            // 计算差异
            long difference = Math.abs(sourceCount - targetCount);
            double differencePercent = sourceCount > 0 ? (double) difference / sourceCount * 100 : 0;

            if (sourceCount == targetCount) {
                result.setSuccess(true);
                result.setMessage(String.format("验证成功，源索引和目标索引文档数量完全一致: %d", sourceCount));
            } else if (differencePercent <= 1.0) { // 允许1%的误差
                result.setSuccess(true);
                result.setMessage(String.format("验证通过，文档数量基本一致 - 源索引: %d, 目标索引: %d, 差异: %d (%.2f%%)",
                        sourceCount, targetCount, difference, differencePercent));
            } else {
                result.setSuccess(false);
                result.setMessage(String.format("验证失败，文档数量差异较大 - 源索引: %d, 目标索引: %d, 差异: %d (%.2f%%)",
                        sourceCount, targetCount, difference, differencePercent));
            }

            log.info("验证结果: {}", result.getMessage());

        } catch (Exception e) {
            log.error("验证迁移结果失败", e);
            result.setSuccess(false);
            result.setMessage("验证失败: " + e.getMessage());
        }

        return result;
    }

    @Override
    public Date getLastSyncTime(String indexName, String timestampField) {
        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.sort(timestampField, SortOrder.DESC);
            searchSourceBuilder.size(1);
            // 虽然只取1条记录，但也设置trackTotalHits以保持一致性
            searchSourceBuilder.trackTotalHits(true);
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = targetClient.search(searchRequest, elasticsearchConfig.getCustomRequestOptions());
            SearchHit[] hits = searchResponse.getHits().getHits();

            if (hits.length > 0) {
                Object timestamp = hits[0].getSourceAsMap().get(timestampField);
                if (timestamp != null) {
                    if (timestamp instanceof String) {
                        return DATE_FORMAT.parse((String) timestamp);
                    } else if (timestamp instanceof Long) {
                        return new Date((Long) timestamp);
                    }
                }
            }
        } catch (Exception e) {
            log.error("获取最后同步时间失败", e);
        }

        return null;
    }

    /**
     * 适配索引映射
     */
    private String adaptMapping(String sourceMapping, MigrationConfig config) {
        try {
            JSONObject mappingJson = JSON.parseObject(sourceMapping);

            // 这里可以根据配置调整映射
            // 例如添加新字段、修改字段类型等
            if (config.getNewFields() != null) {
                JSONObject properties = mappingJson.getJSONObject("mappings")
                        .getJSONObject("properties");

                for (Map.Entry<String, Object> entry : config.getNewFields().entrySet()) {
                    String fieldName = entry.getKey();
                    Object defaultValue = entry.getValue();

                    // 根据默认值类型推断字段类型
                    JSONObject fieldMapping = new JSONObject();
                    if (defaultValue instanceof String) {
                        fieldMapping.put("type", "text");
                        fieldMapping.put("fields", new JSONObject().fluentPut("keyword",
                                new JSONObject().fluentPut("type", "keyword")));
                    } else if (defaultValue instanceof Number) {
                        fieldMapping.put("type", "long");
                    } else if (defaultValue instanceof Boolean) {
                        fieldMapping.put("type", "boolean");
                    } else {
                        fieldMapping.put("type", "text");
                    }

                    properties.put(fieldName, fieldMapping);
                }
            }

            return mappingJson.toJSONString();
        } catch (Exception e) {
            log.error("适配索引映射失败", e);
            return sourceMapping;
        }
    }

    /**
     * 获取大数据量索引的精确文档数量
     * 专门处理超过10万条文档的情况
     */
    public long getLargeIndexDocumentCount(String indexName, boolean isTarget) {
        try {
            RestHighLevelClient client = isTarget ? targetClient : sourceClient;

            // 使用count API获取精确数量，这比search API更高效
            SearchRequest countRequest = new SearchRequest(indexName);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            sourceBuilder.query(QueryBuilders.matchAllQuery());
            sourceBuilder.size(0); // 不返回文档内容，只返回计数
            sourceBuilder.trackTotalHits(true); // 关键：获取精确总数
            sourceBuilder.timeout(TimeValue.timeValueMinutes(5)); // 5分钟超时

            countRequest.source(sourceBuilder);

            log.info("开始获取大数据量索引 {} 的精确文档数量", indexName);

            SearchResponse response = client.search(countRequest, elasticsearchConfig.getLargeQueryRequestOptions());

            long totalHits = response.getHits().getTotalHits().value;

            log.info("索引 {} 的精确文档总数: {} (耗时: {}ms)",
                    indexName, totalHits, response.getTook().getMillis());

            return totalHits;

        } catch (Exception e) {
            log.error("获取大数据量索引 {} 的文档数量失败，回退到普通方法: {}", indexName, e.getMessage());
            // 回退到普通的文档计数方法
            return getDocumentCount(indexName, isTarget);
        }
    }

    /**
     * 智能获取文档数量
     * 根据索引大小自动选择最适合的计数方法
     */
    public long getSmartDocumentCount(String indexName, boolean isTarget) {
        // 先用普通方法快速获取一个估算值
        long estimatedCount = getDocumentCount(indexName, isTarget);

        // 如果估算值接近10000，可能是被限制了，使用精确计数方法
        if (estimatedCount >= 9999) {
            log.info("检测到可能的大数据量索引 {}，使用精确计数方法", indexName);
            return getLargeIndexDocumentCount(indexName, isTarget);
        }

        return estimatedCount;
    }
    
    /**
     * 执行分片迁移
     */
    private MigrationResult executeShardMigration(MigrationConfig config, int shardIndex, long startOffset, long endOffset) throws IOException {
        MigrationResult result = new MigrationResult();
        String threadName = "Migration-Shard-" + shardIndex;
        Thread.currentThread().setName(threadName);
        
        log.info("分片 {} 开始迁移，文档范围: {} - {}", shardIndex, startOffset, endOffset);
        
        SearchRequest searchRequest = new SearchRequest(config.getSourceIndex());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        
        // 动态调整批次大小
        int dynamicBatchSize = adjustBatchSizeForShard(config.getBatchSize(), shardIndex);
        searchSourceBuilder.size(dynamicBatchSize);
        searchSourceBuilder.trackTotalHits(true);
        
        // 添加分片排序，确保数据分布均匀
        searchSourceBuilder.sort("_doc");
        
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(config.getScrollTimeout()));

        SearchResponse searchResponse = esQueryHelper.searchWithRetry(sourceClient, searchRequest, true, 3);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        
        long processedInShard = 0;
        long targetDocsInShard = endOffset - startOffset;
        
        while (searchHits != null && searchHits.length > 0 && processedInShard < targetDocsInShard) {
            // 如果超过了分片目标数量，只处理剩余的
            int actualProcessCount = (int) Math.min(searchHits.length, targetDocsInShard - processedInShard);
            SearchHit[] actualHits = Arrays.copyOf(searchHits, actualProcessCount);
            
            processShardBatch(actualHits, config, result, shardIndex);
            processedInShard += actualProcessCount;
            
            // 记录分片进度
            double progress = (double) processedInShard / targetDocsInShard * 100;
            if (processedInShard % (dynamicBatchSize * 10) == 0) {
                log.info("分片 {} 进度: {:.1f}% ({}/{} docs)", shardIndex, progress, processedInShard, targetDocsInShard);
            }
            
            if (processedInShard >= targetDocsInShard) {
                break;
            }

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueMinutes(config.getScrollTimeout()));
            
            try {
                searchResponse = esQueryHelper.scrollWithRetry(sourceClient, scrollRequest, true, 3);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            } catch (IOException e) {
                log.error("分片 {} 滚动查询失败: {}", shardIndex, e.getMessage());
                throw e;
            }
        }
        
        log.info("分片 {} 迁移完成，处理文档数: {}, 成功: {}, 失败: {}", 
                shardIndex, processedInShard, result.getSuccessCount(), result.getFailedCount());
        
        return result;
    }
    
    /**
     * 处理分片批次数据
     */
    private void processShardBatch(SearchHit[] hits, MigrationConfig config, MigrationResult result, int shardIndex) {
        BulkRequest bulkRequest = new BulkRequest();
        AtomicLong batchSuccessCount = new AtomicLong(0);
        AtomicLong batchFailedCount = new AtomicLong(0);
        
        for (SearchHit hit : hits) {
            try {
                Map<String, Object> sourceData = hit.getSourceAsMap();
                Map<String, Object> targetData = transformDocument(sourceData, config);
                
                IndexRequest indexRequest = new IndexRequest(config.getTargetIndex())
                        .id(hit.getId())
                        .source(targetData, XContentType.JSON);
                
                bulkRequest.add(indexRequest);
                
            } catch (Exception e) {
                log.error("分片 {} 处理文档失败，ID: {}, 错误: {}", shardIndex, hit.getId(), e.getMessage());
                batchFailedCount.incrementAndGet();
            }
        }
        
        if (bulkRequest.numberOfActions() > 0) {
            try {
                BulkResponse bulkResponse = targetClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                
                // 修复错误计数逻辑
                if (bulkResponse.hasFailures()) {
                    Arrays.stream(bulkResponse.getItems())
                        .forEach(item -> {
                            if (item.isFailed()) {
                                batchFailedCount.incrementAndGet();
                                log.warn("分片 {} 文档写入失败: {}, 原因: {}", 
                                    shardIndex, item.getId(), item.getFailureMessage());
                            } else {
                                batchSuccessCount.incrementAndGet();
                            }
                        });
                } else {
                    batchSuccessCount.addAndGet(bulkResponse.getItems().length);
                }
                
                result.setSuccessCount(result.getSuccessCount() + batchSuccessCount.get());
                result.setFailedCount(result.getFailedCount() + batchFailedCount.get());
                
            } catch (IOException e) {
                log.error("分片 {} 批量写入失败", shardIndex, e);
                result.setFailedCount(result.getFailedCount() + bulkRequest.numberOfActions());
            }
        }
    }
    
    /**
     * 为分片调整批次大小
     */
    private int adjustBatchSizeForShard(int baseBatchSize, int shardIndex) {
        // 不同分片使用略有不同的批次大小，避免同时竞争资源
        int adjustment = (shardIndex % 3 - 1) * 100; // -100, 0, +100
        return Math.max(baseBatchSize + adjustment, 100);
    }
    
    /**
     * 带断点续传的单线程迁移
     */
    private void executeSingleThreadMigrationWithCheckpoint(MigrationConfig config, MigrationResult result, 
                                                           String taskId, Optional<MigrationCheckpoint> checkpointOpt) throws IOException {
        MigrationCheckpoint checkpoint = checkpointOpt.orElseGet(() -> {
            MigrationCheckpoint newCheckpoint = new MigrationCheckpoint();
            newCheckpoint.setTaskId(taskId);
            newCheckpoint.setSourceIndex(config.getSourceIndex());
            newCheckpoint.setTargetIndex(config.getTargetIndex());
            newCheckpoint.setTotalCount(result.getTotalCount());
            newCheckpoint.setCurrentBatchSize(config.getBatchSize());
            newCheckpoint.setStatus("RUNNING");
            newCheckpoint.setConfigSnapshot(JSON.toJSONString(config));
            return newCheckpoint;
        });
        
        try {
            log.info("开始单线程迁移，任务ID: {}", taskId);
            executeSingleThreadMigration(config, result);
            
            // 更新最终状态
            checkpoint.setProcessedCount(result.getSuccessCount() + result.getFailedCount());
            checkpoint.setSuccessCount(result.getSuccessCount());
            checkpoint.setFailedCount(result.getFailedCount());
            checkpoint.setStatus("COMPLETED");
            checkpointService.saveCheckpoint(checkpoint);
            
            log.info("单线程迁移成功完成，任务ID: {}", taskId);
            
        } catch (Exception e) {
            log.error("单线程迁移失败，任务ID: {}, 错误: {}", taskId, e.getMessage());
            checkpoint.setStatus("FAILED");
            checkpoint.setErrorMessage(e.getMessage());
            checkpointService.saveCheckpoint(checkpoint);
            throw e;
        }
    }
    
    /**
     * 带断点续传的并行迁移
     */
    private void executeParallelMigrationWithCheckpoint(MigrationConfig config, MigrationResult result, 
                                                        String taskId, Optional<MigrationCheckpoint> checkpointOpt) throws IOException {
        try {
            executeParallelMigration(config, result);
        } catch (Exception e) {
            // 保存并行迁移失败信息
            MigrationCheckpoint checkpoint = new MigrationCheckpoint();
            checkpoint.setTaskId(taskId);
            checkpoint.setSourceIndex(config.getSourceIndex());
            checkpoint.setTargetIndex(config.getTargetIndex());
            checkpoint.setStatus("FAILED");
            checkpoint.setErrorMessage(e.getMessage());
            checkpointService.saveCheckpoint(checkpoint);
            throw e;
        }
    }
    
    /**
     * 生成任务ID
     */
    private String generateTaskId(MigrationConfig config) {
        return String.format("migration_%s_%s_%d", 
            config.getSourceIndex(), 
            config.getTargetIndex(), 
            System.currentTimeMillis());
    }
}
