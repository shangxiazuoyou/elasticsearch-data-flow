package com.everflowx.esmigration.service;

import com.everflowx.esmigration.domain.MigrationConfig;
import com.everflowx.esmigration.pagination.PaginationMigrationEngine;
import com.everflowx.esmigration.resilience.CircuitBreakerManager;
import com.everflowx.esmigration.scroll.EnhancedScrollManager;
import com.everflowx.esmigration.stream.StreamingMigrationEngine;
import com.everflowx.esmigration.throttle.AdaptiveThrottleController;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 无限大数据量迁移服务 - 终极解决方案
 * 
 * 核心目标：无论数据量多大，都能稳定完成迁移
 * 
 * 技术特性：
 * 1. 多策略选择：自动选择最适合的迁移策略
 * 2. 内存安全：严格控制内存使用，防止OOM
 * 3. 自适应限流：根据系统负载动态调整
 * 4. 故障恢复：熔断器保护，自动重试
 * 5. 流式处理：数据流动，不在内存累积
 * 6. 分页迁移：支持TB级数据无限迁移
 * 
 * @author everflowx
 */
@Slf4j
@Service
public class UnlimitedMigrationService {
    
    @Resource
    private StreamingMigrationEngine streamingEngine;
    
    @Resource
    private PaginationMigrationEngine paginationEngine;
    
    @Resource
    private EnhancedScrollManager scrollManager;
    
    @Resource
    private AdaptiveThrottleController throttleController;
    
    @Resource
    private CircuitBreakerManager circuitBreakerManager;
    
    /**
     * 启动无限大数据量迁移
     * 自动选择最适合的策略，确保无论多大数据都能完成
     */
    public UnlimitedMigrationResult startUnlimitedMigration(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config) {
        
        log.info("=== 启动无限大数据量迁移 ===");
        log.info("源索引: {} -> 目标索引: {}", config.getSourceIndex(), config.getTargetIndex());
        log.info("线程数: {}, 批次大小: {}", config.getThreadCount(), config.getBatchSize());
        
        UnlimitedMigrationResult result = new UnlimitedMigrationResult();
        long startTime = System.currentTimeMillis();
        
        try {
            // 1. 智能策略选择
            MigrationStrategy strategy = selectBestStrategy(sourceClient, config);
            log.info("选择迁移策略: {}", strategy);
            result.setStrategy(strategy);
            
            // 2. 执行相应的迁移策略
            switch (strategy) {
                case STREAMING_FLOW:
                    executeStreamingMigration(sourceClient, targetClient, config, result);
                    break;
                case PAGINATION:
                    executePaginationMigration(sourceClient, targetClient, config, result);
                    break;
                case ENHANCED_SCROLL:
                    executeEnhancedScrollMigration(sourceClient, targetClient, config, result);
                    break;
                case HYBRID:
                    executeHybridMigration(sourceClient, targetClient, config, result);
                    break;
            }
            
            if (result.getErrorMessage() == null) {
                result.setSuccess(true);
                log.info("✅ 无限大数据量迁移成功完成");
            } else {
                log.error("❌ 无限大数据量迁移失败: {}", result.getErrorMessage());
            }
            
        } catch (Exception e) {
            log.error("无限大数据量迁移异常", e);
            result.setErrorMessage(e.getMessage());
        } finally {
            long totalTime = System.currentTimeMillis() - startTime;
            result.setTotalTimeMs(totalTime);
            
            // 输出最终统计
            logFinalStatistics(result, totalTime);
            
            // 清理资源
            throttleController.resetStats();
        }
        
        return result;
    }
    
    /**
     * 智能选择最佳迁移策略
     */
    private MigrationStrategy selectBestStrategy(RestHighLevelClient sourceClient, MigrationConfig config) {
        try {
            // 获取索引基本信息
            long estimatedDocCount = getEstimatedDocCount(sourceClient, config.getSourceIndex());
            boolean hasTimeField = hasTimeField(sourceClient, config.getSourceIndex(), config.getTimestampField());
            double currentMemoryPressure = getCurrentMemoryPressure();
            
            log.info("索引分析 - 预估文档数: {}, 有时间字段: {}, 当前内存压力: {:.1f%}", 
                estimatedDocCount, hasTimeField, currentMemoryPressure * 100);
            
            // 策略选择逻辑
            if (estimatedDocCount > 100_000_000) { // 超过1亿文档
                if (hasTimeField) {
                    log.info("超大数据集且有时间字段，选择分页策略");
                    return MigrationStrategy.PAGINATION;
                } else {
                    log.info("超大数据集无时间字段，选择混合策略");
                    return MigrationStrategy.HYBRID;
                }
            } else if (estimatedDocCount > 10_000_000) { // 超过1000万文档
                if (currentMemoryPressure > 0.7) {
                    log.info("大数据集且内存压力大，选择流式处理");
                    return MigrationStrategy.STREAMING_FLOW;
                } else {
                    log.info("大数据集内存充足，选择增强Scroll");
                    return MigrationStrategy.ENHANCED_SCROLL;
                }
            } else { // 中小数据集
                log.info("中小数据集，选择流式处理");
                return MigrationStrategy.STREAMING_FLOW;
            }
            
        } catch (Exception e) {
            log.warn("策略选择异常，使用默认混合策略: {}", e.getMessage());
            return MigrationStrategy.HYBRID;
        }
    }
    
    /**
     * 执行流式迁移
     */
    private void executeStreamingMigration(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config,
            UnlimitedMigrationResult result) {
        
        log.info("🌊 执行流式迁移策略");
        
        // 创建文档供应商
        StreamingDocumentSupplier supplier = new StreamingDocumentSupplier(sourceClient, config);
        
        // 创建文档转换器
        StreamingDocumentTransformer transformer = new StreamingDocumentTransformer();
        
        // 启动流式迁移
        StreamingMigrationEngine.StreamingMigrationResult streamResult = 
            streamingEngine.startStreaming(sourceClient, targetClient, config, supplier, transformer);
        
        // 转换结果
        result.setProcessedCount(streamResult.getProcessedCount());
        result.setSuccessCount(streamResult.getSuccessCount());
        result.setFailedCount(streamResult.getFailedCount());
        result.setErrorMessage(streamResult.getErrorMessage());
    }
    
    /**
     * 执行分页迁移
     */
    private void executePaginationMigration(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config,
            UnlimitedMigrationResult result) {
        
        log.info("📄 执行分页迁移策略");
        
        // 配置分页参数
        PaginationMigrationEngine.PaginationConfig paginationConfig = 
            new PaginationMigrationEngine.PaginationConfig()
                .strategy(PaginationMigrationEngine.PaginationStrategy.HYBRID)
                .pageSize(throttleController.getAdaptiveBatchSize())
                .timeField(config.getTimestampField())
                .timeWindowMinutes(10);
        
        // 创建分页回调
        PaginationCallback callback = new PaginationCallback(targetClient, config, result);
        
        // 启动分页迁移
        PaginationMigrationEngine.PaginationResult paginationResult = 
            paginationEngine.startPagination(sourceClient, config, callback, paginationConfig);
        
        // 转换结果
        result.setProcessedCount(paginationResult.getProcessedCount());
        result.setSuccessCount(paginationResult.getSuccessCount());
        result.setFailedCount(paginationResult.getFailedCount());
        result.setTotalPages(paginationResult.getTotalPages());
        result.setErrorMessage(paginationResult.getErrorMessage());
    }
    
    /**
     * 执行增强Scroll迁移
     */
    private void executeEnhancedScrollMigration(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config,
            UnlimitedMigrationResult result) {
        
        log.info("🔄 执行增强Scroll迁移策略");
        
        // 创建初始搜索请求
        org.elasticsearch.action.search.SearchRequest searchRequest = new org.elasticsearch.action.search.SearchRequest(config.getSourceIndex());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.size(throttleController.getAdaptiveBatchSize());
        sourceBuilder.trackTotalHits(true);
        searchRequest.source(sourceBuilder);
        
        // 创建Scroll回调
        ScrollCallback callback = new ScrollCallback(targetClient, config, result);
        
        // 配置Scroll参数
        EnhancedScrollManager.ScrollConfig scrollConfig = new EnhancedScrollManager.ScrollConfig()
            .estimatedTotalDocs(10_000_000L) // 假设1000万文档
            .initialBatchSize(config.getBatchSize())
            .enableAdaptiveBatching(true)
            .enableMemoryPressureControl(true);
        
        // 启动增强Scroll
        scrollManager.startInfiniteScroll(sourceClient, searchRequest, callback, scrollConfig);
    }
    
    /**
     * 执行混合迁移策略
     */
    private void executeHybridMigration(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config,
            UnlimitedMigrationResult result) {
        
        log.info("🔄 执行混合迁移策略");
        
        try {
            // 先尝试分页迁移
            executePaginationMigration(sourceClient, targetClient, config, result);
            
            // 如果分页迁移失败，回退到流式迁移
            if (result.getErrorMessage() != null) {
                log.warn("分页迁移失败，回退到流式迁移: {}", result.getErrorMessage());
                
                // 重置结果
                result = new UnlimitedMigrationResult();
                result.setStrategy(MigrationStrategy.HYBRID);
                
                executeStreamingMigration(sourceClient, targetClient, config, result);
            }
        } catch (Exception e) {
            log.error("混合策略执行失败", e);
            result.setErrorMessage("混合策略失败: " + e.getMessage());
        }
    }
    
    /**
     * 获取预估文档数量
     */
    private long getEstimatedDocCount(RestHighLevelClient client, String indexName) {
        try {
            org.elasticsearch.action.search.SearchRequest request = new org.elasticsearch.action.search.SearchRequest(indexName);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.size(0);
            sourceBuilder.trackTotalHits(true);
            request.source(sourceBuilder);
            
            org.elasticsearch.action.search.SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            return response.getHits().getTotalHits().value;
        } catch (Exception e) {
            log.warn("获取文档数量失败，使用默认值: {}", e.getMessage());
            return 1000000; // 默认100万
        }
    }
    
    /**
     * 检查是否有时间字段
     */
    private boolean hasTimeField(RestHighLevelClient client, String indexName, String timeField) {
        if (timeField == null) return false;
        
        try {
            org.elasticsearch.action.search.SearchRequest request = new org.elasticsearch.action.search.SearchRequest(indexName);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.existsQuery(timeField));
            sourceBuilder.size(1);
            request.source(sourceBuilder);
            
            org.elasticsearch.action.search.SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            return response.getHits().getTotalHits().value > 0;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * 获取当前内存压力
     */
    private double getCurrentMemoryPressure() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long maxMemory = runtime.maxMemory();
        
        long usedMemory = totalMemory - freeMemory;
        return (double) usedMemory / maxMemory;
    }
    
    /**
     * 记录最终统计信息
     */
    private void logFinalStatistics(UnlimitedMigrationResult result, long totalTime) {
        double speed = totalTime > 0 ? (double) result.getProcessedCount() / totalTime * 1000 : 0;
        double successRate = result.getProcessedCount() > 0 ? 
            (double) result.getSuccessCount() / result.getProcessedCount() * 100 : 0;
        
        log.info("\n" +
            "========== 无限大数据量迁移完成 ==========\n" +
            "策略: {}\n" +
            "总耗时: {} ms ({} 分钟)\n" +
            "处理文档: {}\n" +
            "成功文档: {}\n" +
            "失败文档: {}\n" +
            "总页数: {}\n" +
            "成功率: {:.2f}%\n" +
            "平均速度: {:.1f} docs/s\n" +
            "限流统计: {}\n" +
            "熔断器状态: {}\n" +
            "===========================================",
            result.getStrategy(),
            totalTime, totalTime / 60000,
            result.getProcessedCount(),
            result.getSuccessCount(),
            result.getFailedCount(),
            result.getTotalPages(),
            successRate,
            speed,
            throttleController.getStats(),
            circuitBreakerManager.getAllBreakerStatus()
        );
    }
    
    /**
     * 流式文档供应商实现
     */
    private class StreamingDocumentSupplier implements StreamingMigrationEngine.DocumentSupplier {
        private final RestHighLevelClient client;
        private final MigrationConfig config;
        private String scrollId;
        private boolean isFirstBatch = true;
        
        public StreamingDocumentSupplier(RestHighLevelClient client, MigrationConfig config) {
            this.client = client;
            this.config = config;
        }
        
        @Override
        public SearchHit[] nextBatch(int batchSize) throws Exception {
            if (isFirstBatch) {
                isFirstBatch = false;
                
                org.elasticsearch.action.search.SearchRequest request = new org.elasticsearch.action.search.SearchRequest(config.getSourceIndex());
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
                sourceBuilder.query(QueryBuilders.matchAllQuery());
                sourceBuilder.size(batchSize);
                sourceBuilder.trackTotalHits(true);
                request.source(sourceBuilder);
                request.scroll(org.elasticsearch.common.unit.TimeValue.timeValueMinutes(5));
                
                org.elasticsearch.action.search.SearchResponse response = client.search(request, RequestOptions.DEFAULT);
                scrollId = response.getScrollId();
                return response.getHits().getHits();
            } else {
                if (scrollId == null) return null;
                
                org.elasticsearch.action.search.SearchScrollRequest scrollRequest = new org.elasticsearch.action.search.SearchScrollRequest(scrollId);
                scrollRequest.scroll(org.elasticsearch.common.unit.TimeValue.timeValueMinutes(5));
                
                org.elasticsearch.action.search.SearchResponse response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = response.getScrollId();
                SearchHit[] hits = response.getHits().getHits();
                
                return hits.length > 0 ? hits : null;
            }
        }
    }
    
    /**
     * 流式文档转换器实现
     */
    private class StreamingDocumentTransformer implements StreamingMigrationEngine.DocumentTransformer {
        @Override
        public Map<String, Object> transform(Map<String, Object> sourceDoc, MigrationConfig config) throws Exception {
            // 这里可以实现具体的文档转换逻辑
            // 暂时直接返回原文档
            return sourceDoc;
        }
    }
    
    /**
     * 分页回调实现
     */
    private class PaginationCallback implements PaginationMigrationEngine.PaginationCallback {
        private final RestHighLevelClient targetClient;
        private final MigrationConfig config;
        private final UnlimitedMigrationResult result;
        
        public PaginationCallback(RestHighLevelClient targetClient, MigrationConfig config, UnlimitedMigrationResult result) {
            this.targetClient = targetClient;
            this.config = config;
            this.result = result;
        }
        
        @Override
        public void processPage(SearchHit[] hits, long pageNumber, String pageKey) throws Exception {
            AdaptiveThrottleController.ThrottlePermit permit = throttleController.acquirePermit();
            try {
                processBatchWithThrottle(hits, permit);
            } finally {
                permit.release();
            }
        }
        
        private void processBatchWithThrottle(SearchHit[] hits, AdaptiveThrottleController.ThrottlePermit permit) throws Exception {
            BulkRequest bulkRequest = new BulkRequest();
            
            for (SearchHit hit : hits) {
                Map<String, Object> sourceData = hit.getSourceAsMap();
                IndexRequest indexRequest = new IndexRequest(config.getTargetIndex())
                    .id(hit.getId())
                    .source(sourceData, XContentType.JSON);
                bulkRequest.add(indexRequest);
            }
            
            BulkResponse bulkResponse = targetClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            
            if (bulkResponse.hasFailures()) {
                Arrays.stream(bulkResponse.getItems()).forEach(item -> {
                    if (item.isFailed()) {
                        result.addFailedCount(1);
                    } else {
                        result.addSuccessCount(1);
                    }
                });
            } else {
                result.addSuccessCount(hits.length);
            }
            
            permit.updateProcessedDocs(hits.length);
        }
    }
    
    /**
     * Scroll回调实现
     */
    private class ScrollCallback implements EnhancedScrollManager.ScrollCallback {
        private final RestHighLevelClient targetClient;
        private final MigrationConfig config;
        private final UnlimitedMigrationResult result;
        
        public ScrollCallback(RestHighLevelClient targetClient, MigrationConfig config, UnlimitedMigrationResult result) {
            this.targetClient = targetClient;
            this.config = config;
            this.result = result;
        }
        
        @Override
        public void processHits(SearchHit[] hits, long scrollRound) throws Exception {
            AdaptiveThrottleController.ThrottlePermit permit = throttleController.acquirePermit();
            try {
                processBatchWithThrottle(hits, permit);
            } finally {
                permit.release();
            }
        }
        
        private void processBatchWithThrottle(SearchHit[] hits, AdaptiveThrottleController.ThrottlePermit permit) throws Exception {
            BulkRequest bulkRequest = new BulkRequest();
            
            for (SearchHit hit : hits) {
                Map<String, Object> sourceData = hit.getSourceAsMap();
                IndexRequest indexRequest = new IndexRequest(config.getTargetIndex())
                    .id(hit.getId())
                    .source(sourceData, XContentType.JSON);
                bulkRequest.add(indexRequest);
            }
            
            BulkResponse bulkResponse = targetClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            
            if (bulkResponse.hasFailures()) {
                Arrays.stream(bulkResponse.getItems()).forEach(item -> {
                    if (item.isFailed()) {
                        result.addFailedCount(1);
                    } else {
                        result.addSuccessCount(1);
                    }
                });
            } else {
                result.addSuccessCount(hits.length);
            }
            
            permit.updateProcessedDocs(hits.length);
        }
    }
    
    /**
     * 迁移策略枚举
     */
    public enum MigrationStrategy {
        STREAMING_FLOW,    // 流式处理
        PAGINATION,        // 分页迁移
        ENHANCED_SCROLL,   // 增强Scroll
        HYBRID            // 混合策略
    }
    
    /**
     * 无限大数据量迁移结果
     */
    @Data
    public static class UnlimitedMigrationResult {
        private MigrationStrategy strategy;
        private final AtomicLong processedCount = new AtomicLong(0);
        private final AtomicLong successCount = new AtomicLong(0);
        private final AtomicLong failedCount = new AtomicLong(0);
        private long totalPages;
        private String errorMessage;
        private long totalTimeMs;
        private boolean success = false;
        
        public void addProcessedCount(long count) { processedCount.addAndGet(count); }
        public void addSuccessCount(long count) { successCount.addAndGet(count); }
        public void addFailedCount(long count) { failedCount.addAndGet(count); }
        
        public void setProcessedCount(long count) { processedCount.set(count); }
        public void setSuccessCount(long count) { successCount.set(count); }
        public void setFailedCount(long count) { failedCount.set(count); }
        
        public long getProcessedCount() { return processedCount.get(); }
        public long getSuccessCount() { return successCount.get(); }
        public long getFailedCount() { return failedCount.get(); }
    }
}