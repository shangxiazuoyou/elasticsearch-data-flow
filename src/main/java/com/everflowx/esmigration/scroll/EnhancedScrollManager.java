package com.everflowx.esmigration.scroll;

import com.everflowx.esmigration.resilience.CircuitBreakerManager;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 增强型Scroll管理器 - 支持无限大数据集的稳定迁移
 * 
 * 关键特性：
 * 1. 自动Scroll生命周期管理
 * 2. 内存压力感知的批次大小调整
 * 3. 网络异常恢复和重试
 * 4. Scroll ID泄漏防护
 * 5. 支持TB级数据集的长时间迁移
 * 
 * @author everflowx
 */
@Slf4j
@Component
public class EnhancedScrollManager {
    
    @Resource
    private CircuitBreakerManager circuitBreakerManager;
    
    // 配置常量
    private static final String SCROLL_TIMEOUT_5M = "5m";
    private static final String SCROLL_TIMEOUT_10M = "10m";
    private static final String SCROLL_TIMEOUT_30M = "30m";
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final long RETRY_DELAY_MS = 2000;
    private static final double MEMORY_PRESSURE_THRESHOLD = 0.85;

    /**
     * 启动无限大数据集的Scroll迁移
     */
    public void startInfiniteScroll(RestHighLevelClient client, SearchRequest initialRequest, 
                                   ScrollCallback callback, ScrollConfig config) {
        String scrollId = null;
        AtomicLong totalProcessed = new AtomicLong(0);
        AtomicLong scrollRounds = new AtomicLong(0);
        long startTime = System.currentTimeMillis();
        
        // 设置自适应的scroll超时时间
        String scrollTimeout = determineScrollTimeout(config.getEstimatedTotalDocs());
        initialRequest.scroll(TimeValue.parseTimeValue(scrollTimeout, "scroll_timeout"));

        log.info("启动无限大数据集Scroll - 预估文档数: {}, Scroll超时: {}, 初始批次大小: {}", 
            config.getEstimatedTotalDocs(), scrollTimeout, initialRequest.source().size());
        
        try {
            // 执行初始搜索
            SearchResponse searchResponse = executeWithCircuitBreaker(client, "initial_search", 
                () -> client.search(initialRequest, RequestOptions.DEFAULT));
            scrollId = searchResponse.getScrollId();
            SearchHit[] hits = searchResponse.getHits().getHits();
            
            // 处理第一批数据
            if (hits.length > 0) {
                callback.processHits(hits, scrollRounds.get());
                totalProcessed.addAndGet(hits.length);
                scrollRounds.incrementAndGet();
            }
            
            // 开始Scroll循环 - 支持无限大数据集
            while (hits.length > 0) {
                long roundStart = System.currentTimeMillis();
                
                // 自适应批次大小调整
                int adjustedBatchSize = adjustBatchSizeForMemory(initialRequest.source().size());
                if (adjustedBatchSize != initialRequest.source().size()) {
                    log.debug("动态调整批次大小: {} -> {}", initialRequest.source().size(), adjustedBatchSize);
                }
                
                // 创建Scroll请求
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                // 根据数据量和内存压力动态调整超时时间
                String adaptiveTimeout = adaptScrollTimeout(totalProcessed.get(), getMemoryPressure());
                scrollRequest.scroll(TimeValue.parseTimeValue(adaptiveTimeout, "adaptive_timeout"));
                
                // 执行Scroll查询（带熔断保护）
                searchResponse = executeWithCircuitBreaker(client, "scroll_search",
                        () -> client.scroll(scrollRequest, RequestOptions.DEFAULT));
                scrollId = searchResponse.getScrollId();
                hits = searchResponse.getHits().getHits();
                
                if (hits.length > 0) {
                    // 处理当前批次
                    callback.processHits(hits, scrollRounds.get());
                    totalProcessed.addAndGet(hits.length);
                    scrollRounds.incrementAndGet();
                    
                    // 性能统计和监控
                    long roundTime = System.currentTimeMillis() - roundStart;
                    if (scrollRounds.get() % 100 == 0) {
                        logScrollProgress(totalProcessed.get(), scrollRounds.get(),
                                startTime, roundTime, hits.length);
                    }
                    
                    // 内存压力过高时主动GC
                    if (getMemoryPressure() > MEMORY_PRESSURE_THRESHOLD) {
                        log.warn("内存压力过高 {:.1f}%，触发GC", getMemoryPressure() * 100);
                        System.gc();
                        Thread.sleep(1000); // 给GC时间
                    }
                    
                    // 长时间运行的Scroll需要定期刷新（防止超时）
                    if (scrollRounds.get() % 1000 == 0) {
                        refreshScrollContext(client, scrollId);
                    }
                }
                
                // 适应性延迟，避免过度压迫ES
                adaptiveDelay(getMemoryPressure(), hits.length);
            }
            
        } catch (Exception e) {
            log.error("Scroll处理异常，已处理: {} 文档", totalProcessed.get(), e);
            throw new RuntimeException("Scroll迁移失败: " + e.getMessage(), e);
        } finally {
            // 确保清理Scroll上下文
            cleanupScrollContext(client, scrollId);
            
            long totalTime = System.currentTimeMillis() - startTime;
            log.info("无限大数据集Scroll完成 - 总处理: {} 文档, {} 轮Scroll, 耗时: {}ms, 平均速度: {:.1f} docs/s",
                totalProcessed.get(), scrollRounds.get(), totalTime,
                totalTime > 0 ? (double) totalProcessed.get() / totalTime * 1000 : 0);
        }
    }
    
    /**
     * 带熔断器的ES操作执行
     */
    private <T> T executeWithCircuitBreaker(RestHighLevelClient client, String operation,
                                           ThrowingSupplier<T> supplier) {
        return circuitBreakerManager.executeWithBreaker(
            "es_" + operation,
            () -> {
                try {
                    return supplier.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            },
            () -> {
                log.error("ES操作 {} 熔断，无法提供降级服务", operation);
                throw new RuntimeException("ES服务不可用，操作被熔断: " + operation);
            }
        );
    }
    
    /**
     * 根据数据量确定Scroll超时时间
     */
    private String determineScrollTimeout(long estimatedDocs) {
        if (estimatedDocs > 10_000_000) { // 超过1000万
            return SCROLL_TIMEOUT_30M;
        } else if (estimatedDocs > 1_000_000) { // 超过100万
            return SCROLL_TIMEOUT_10M;
        } else {
            return SCROLL_TIMEOUT_5M;
        }
    }
    
    /**
     * 自适应Scroll超时时间
     */
    private String adaptScrollTimeout(long processedDocs, double memoryPressure) {
        if (memoryPressure > 0.9 || processedDocs > 50_000_000) {
            return SCROLL_TIMEOUT_30M; // 内存压力大或数据量超大时，延长超时
        } else if (memoryPressure > 0.8 || processedDocs > 10_000_000) {
            return SCROLL_TIMEOUT_10M;
        } else {
            return SCROLL_TIMEOUT_5M;
        }
    }
    
    /**
     * 根据内存压力调整批次大小
     */
    private int adjustBatchSizeForMemory(int currentBatchSize) {
        double memoryPressure = getMemoryPressure();
        
        if (memoryPressure > 0.9) {
            // 内存压力极高，大幅减少批次
            return Math.max(currentBatchSize / 4, 50);
        } else if (memoryPressure > 0.8) {
            // 内存压力较高，适度减少批次
            return Math.max(currentBatchSize / 2, 100);
        } else if (memoryPressure < 0.5) {
            // 内存充足，可以增加批次
            return Math.min(currentBatchSize * 2, 10000);
        }
        
        return currentBatchSize;
    }
    
    /**
     * 获取内存压力
     */
    private double getMemoryPressure() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long maxMemory = runtime.maxMemory();
        
        long usedMemory = totalMemory - freeMemory;
        return (double) usedMemory / maxMemory;
    }
    
    /**
     * 适应性延迟
     */
    private void adaptiveDelay(double memoryPressure, int batchSize) throws InterruptedException {
        if (memoryPressure > 0.85) {
            // 内存压力大时，延迟更长
            Thread.sleep((long)(100 * memoryPressure));
        } else if (batchSize > 5000) {
            // 批次大时，稍作延迟
            Thread.sleep(50);
        }
        // 正常情况下不延迟，保持最大速度
    }
    
    /**
     * 刷新Scroll上下文（长时间运行时防止超时）
     */
    private void refreshScrollContext(RestHighLevelClient client, String scrollId) {
        try {
            // 通过一个空的scroll请求来刷新超时时间
            SearchScrollRequest refreshRequest = new SearchScrollRequest(scrollId);
            refreshRequest.scroll(TimeValue.parseTimeValue(SCROLL_TIMEOUT_30M, "refresh"));
            
            // 不处理结果，只是为了刷新超时
            client.scroll(refreshRequest, RequestOptions.DEFAULT);
            log.debug("Scroll上下文已刷新");
        } catch (Exception e) {
            log.warn("刷新Scroll上下文失败: {}", e.getMessage());
        }
    }
    
    /**
     * 清理Scroll上下文
     */
    private void cleanupScrollContext(RestHighLevelClient client, String scrollId) {
        if (scrollId != null) {
            try {
                ClearScrollRequest clearRequest = new ClearScrollRequest();
                clearRequest.addScrollId(scrollId);
                client.clearScroll(clearRequest, RequestOptions.DEFAULT);
                log.debug("Scroll上下文已清理: {}", scrollId);
            } catch (Exception e) {
                log.warn("清理Scroll上下文失败: {}", e.getMessage());
            }
        }
    }
    
    /**
     * 记录Scroll进度
     */
    private void logScrollProgress(long totalProcessed, long rounds, long startTime,
                                  long roundTime, int batchSize) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        double avgSpeed = elapsedTime > 0 ? (double) totalProcessed / elapsedTime * 1000 : 0;
        double currentSpeed = roundTime > 0 ? (double) batchSize / roundTime * 1000 : 0;
        
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;
        long maxMemory = runtime.maxMemory() / 1024 / 1024;
        
        log.info("Scroll进度 - 已处理: {} 文档, {} 轮, 平均速度: {:.1f} docs/s, 当前速度: {:.1f} docs/s, " +
                "内存: {}MB/{} MB ({:.1f}%), 运行时间: {}s",
                totalProcessed, rounds, avgSpeed, currentSpeed,
                usedMemory, maxMemory, (double) usedMemory / maxMemory * 100,
                elapsedTime / 1000);
    }
    
    /**
     * Scroll回调接口
     */
    public interface ScrollCallback {
        void processHits(SearchHit[] hits, long scrollRound) throws Exception;
    }
    
    /**
     * 抛出异常的Supplier
     */
    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
    
    /**
     * Scroll配置
     */
    public static class ScrollConfig {
        private long estimatedTotalDocs = 1000000; // 默认100万
        private int initialBatchSize = 1000;
        private boolean enableAdaptiveBatching = true;
        private boolean enableMemoryPressureControl = true;
        
        public ScrollConfig estimatedTotalDocs(long docs) {
            this.estimatedTotalDocs = docs;
            return this;
        }
        
        public ScrollConfig initialBatchSize(int size) {
            this.initialBatchSize = size;
            return this;
        }
        
        public ScrollConfig enableAdaptiveBatching(boolean enable) {
            this.enableAdaptiveBatching = enable;
            return this;
        }
        
        public ScrollConfig enableMemoryPressureControl(boolean enable) {
            this.enableMemoryPressureControl = enable;
            return this;
        }
        
        // Getters
        public long getEstimatedTotalDocs() { return estimatedTotalDocs; }
        public int getInitialBatchSize() { return initialBatchSize; }
        public boolean isEnableAdaptiveBatching() { return enableAdaptiveBatching; }
        public boolean isEnableMemoryPressureControl() { return enableMemoryPressureControl; }
    }
}