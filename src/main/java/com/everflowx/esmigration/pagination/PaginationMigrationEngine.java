package com.everflowx.esmigration.pagination;

import com.everflowx.esmigration.domain.MigrationConfig;
import com.everflowx.esmigration.resilience.CircuitBreakerManager;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 分页式迁移引擎 - 替代Scroll的无限数据量迁移方案
 * 
 * 优势：
 * 1. 无Scroll上下文，不占用ES内存
 * 2. 支持真正的无限大数据集（TB级）
 * 3. 可中断和恢复，更好的容错性
 * 4. 不依赖scroll_id，避免超时问题
 * 5. 支持实时数据变更
 * 
 * 策略：
 * - 时间范围分页：按时间戳字段分页
 * - ID范围分页：按文档ID分页
 * - 混合分页：时间+ID双重排序
 * 
 * @author everflowx
 */
@Slf4j
@Component
public class PaginationMigrationEngine {
    
    @Resource
    private CircuitBreakerManager circuitBreakerManager;
    
    // 分页配置常量
    private static final int DEFAULT_PAGE_SIZE = 2000;
    private static final int MAX_PAGE_SIZE = 10000;
    private static final int MIN_PAGE_SIZE = 100;
    private static final String DEFAULT_TIME_FIELD = "@timestamp";
    private static final String ID_FIELD = "_id";
    
    /**
     * 启动分页式迁移 - 支持无限大数据集
     */
    public PaginationResult startPagination(
            RestHighLevelClient sourceClient,
            MigrationConfig config,
            PaginationCallback callback,
            PaginationConfig paginationConfig) {
        
        PaginationResult result = new PaginationResult();
        long startTime = System.currentTimeMillis();
        
        log.info("启动分页式迁移 - 索引: {}, 分页策略: {}, 页大小: {}", 
            config.getSourceIndex(), paginationConfig.getStrategy(), paginationConfig.getPageSize());
        
        try {
            switch (paginationConfig.getStrategy()) {
                case TIME_BASED:
                    paginateByTime(sourceClient, config, callback, paginationConfig, result);
                    break;
                case ID_BASED:
                    paginateById(sourceClient, config, callback, paginationConfig, result);
                    break;
                case HYBRID:
                    paginateByHybrid(sourceClient, config, callback, paginationConfig, result);
                    break;
                default:
                    throw new IllegalArgumentException("不支持的分页策略: " + paginationConfig.getStrategy());
            }
        } catch (Exception e) {
            log.error("分页式迁移异常", e);
            result.setErrorMessage(e.getMessage());
        } finally {
            long totalTime = System.currentTimeMillis() - startTime;
            result.setTotalTimeMs(totalTime);
            
            log.info("分页式迁移完成 - 总页数: {}, 处理文档: {}, 成功: {}, 失败: {}, 耗时: {}ms, 平均速度: {:.1f} docs/s", 
                result.getTotalPages(), result.getProcessedCount(), result.getSuccessCount(), 
                result.getFailedCount(), totalTime, 
                totalTime > 0 ? (double) result.getProcessedCount() / totalTime * 1000 : 0);
        }
        
        return result;
    }
    
    /**
     * 基于时间范围的分页迁移 - 适合有时间字段的数据
     */
    private void paginateByTime(
            RestHighLevelClient sourceClient,
            MigrationConfig config,
            PaginationCallback callback,
            PaginationConfig paginationConfig,
            PaginationResult result) throws Exception {
        
        String timeField = paginationConfig.getTimeField() != null ? 
            paginationConfig.getTimeField() : config.getTimestampField();
        
        if (timeField == null) {
            timeField = DEFAULT_TIME_FIELD;
        }
        
        log.info("使用时间分页策略，时间字段: {}", timeField);
        
        // 获取时间范围
        TimeRange timeRange = getTimeRange(sourceClient, config.getSourceIndex(), timeField);
        log.info("数据时间范围: {} 到 {}", timeRange.getStartTime(), timeRange.getEndTime());
        
        // 按时间范围分页
        LocalDateTime currentTime = timeRange.getStartTime();
        LocalDateTime endTime = timeRange.getEndTime();
        long pageNumber = 0;
        
        while (currentTime.isBefore(endTime)) {
            pageNumber++;
            
            // 计算当前页的时间范围
            LocalDateTime pageEndTime = currentTime.plusMinutes(paginationConfig.getTimeWindowMinutes());
            if (pageEndTime.isAfter(endTime)) {
                pageEndTime = endTime;
            }
            
            // 处理当前时间窗口的数据
            boolean hasMoreData = processTimeWindow(sourceClient, config, callback, paginationConfig,
                result, timeField, currentTime, pageEndTime, pageNumber);
            
            currentTime = pageEndTime;
            
            // 如果当前窗口没有数据且时间窗口较小，扩大窗口
            if (!hasMoreData && paginationConfig.getTimeWindowMinutes() < 60) {
                paginationConfig.setTimeWindowMinutes(paginationConfig.getTimeWindowMinutes() * 2);
                log.debug("扩大时间窗口至 {} 分钟", paginationConfig.getTimeWindowMinutes());
            }
            
            // 内存压力控制
            controlMemoryPressure();
        }
        
        result.setTotalPages(pageNumber);
    }
    
    /**
     * 处理时间窗口内的数据
     */
    private boolean processTimeWindow(
            RestHighLevelClient sourceClient,
            MigrationConfig config,
            PaginationCallback callback,
            PaginationConfig paginationConfig,
            PaginationResult result,
            String timeField,
            LocalDateTime startTime,
            LocalDateTime endTime,
            long pageNumber) throws Exception {
        
        SearchRequest request = new SearchRequest(config.getSourceIndex());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        
        // 时间范围查询
        sourceBuilder.query(
            QueryBuilders.rangeQuery(timeField)
                .gte(startTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .lt(endTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
        );
        
        // 排序和分页
        sourceBuilder.sort(timeField, SortOrder.ASC);
        sourceBuilder.sort("_id", SortOrder.ASC); // 二级排序确保稳定性
        sourceBuilder.size(paginationConfig.getPageSize());
        sourceBuilder.trackTotalHits(true);
        
        request.source(sourceBuilder);
        
        // 执行查询
        SearchResponse response = executeWithCircuitBreaker(sourceClient, request);
        SearchHit[] hits = response.getHits().getHits();
        
        if (hits.length > 0) {
            log.debug("时间窗口 [{} - {}] 页 {} 找到 {} 条记录", 
                startTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                endTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                pageNumber, hits.length);
            
            // 处理当前页数据
            callback.processPage(hits, pageNumber, startTime.toString());
            result.addProcessedCount(hits.length);
            
            // 如果当前页满了，可能还有更多数据，需要深度分页
            if (hits.length == paginationConfig.getPageSize()) {
                processDeepTimeWindow(sourceClient, config, callback, paginationConfig,
                    result, timeField, startTime, endTime, pageNumber);
            }
            
            return true;
        }
        
        return false;
    }
    
    /**
     * 处理时间窗口内的深度分页（当单个时间窗口数据量很大时）
     */
    private void processDeepTimeWindow(
            RestHighLevelClient sourceClient,
            MigrationConfig config,
            PaginationCallback callback,
            PaginationConfig paginationConfig,
            PaginationResult result,
            String timeField,
            LocalDateTime startTime,
            LocalDateTime endTime,
            long basePageNumber) throws Exception {
        
        String lastId = null;
        LocalDateTime lastTime = startTime;
        int deepPageCount = 0;
        
        while (true) {
            deepPageCount++;
            
            SearchRequest request = new SearchRequest(config.getSourceIndex());
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            
            // 构建深度分页查询
            org.elasticsearch.index.query.RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(timeField)
                .gte(lastTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .lt(endTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            
            if (lastId != null) {
                // 使用search_after进行深度分页
                org.elasticsearch.index.query.BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
                    .must(rangeQuery)
                    .should(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(timeField, lastTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)))
                        .must(QueryBuilders.rangeQuery("_id").gt(lastId))
                    );
                sourceBuilder.query(boolQuery);
            } else {
                sourceBuilder.query(rangeQuery);
            }
            
            sourceBuilder.sort(timeField, SortOrder.ASC);
            sourceBuilder.sort("_id", SortOrder.ASC);
            sourceBuilder.size(paginationConfig.getPageSize());
            
            request.source(sourceBuilder);
            
            // 执行查询
            SearchResponse response = executeWithCircuitBreaker(sourceClient, request);
            SearchHit[] hits = response.getHits().getHits();
            
            if (hits.length == 0) {
                break;
            }
            
            // 处理当前深度页数据
            String pageKey = basePageNumber + "-" + deepPageCount;
            callback.processPage(hits, basePageNumber * 1000 + deepPageCount, pageKey);
            result.addProcessedCount(hits.length);
            
            // 更新游标
            SearchHit lastHit = hits[hits.length - 1];
            lastId = lastHit.getId();
            Object timeValue = lastHit.getSourceAsMap().get(timeField);
            if (timeValue != null) {
                lastTime = LocalDateTime.parse(timeValue.toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            }
            
            // 防止无限循环
            if (hits.length < paginationConfig.getPageSize()) {
                break;
            }
            
            if (deepPageCount > 1000) {
                log.warn("深度分页超过1000页，可能存在问题，中断处理");
                break;
            }
        }
        
        if (deepPageCount > 1) {
            log.info("时间窗口深度分页完成，共 {} 页", deepPageCount);
        }
    }
    
    /**
     * 基于ID的分页迁移 - 适合没有时间字段的数据
     */
    private void paginateById(
            RestHighLevelClient sourceClient,
            MigrationConfig config,
            PaginationCallback callback,
            PaginationConfig paginationConfig,
            PaginationResult result) throws Exception {
        
        log.info("使用ID分页策略");
        
        String lastId = null;
        long pageNumber = 0;
        
        while (true) {
            pageNumber++;
            
            SearchRequest request = new SearchRequest(config.getSourceIndex());
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            
            // 构建ID范围查询
            if (lastId != null) {
                sourceBuilder.query(QueryBuilders.rangeQuery("_id").gt(lastId));
            } else {
                sourceBuilder.query(QueryBuilders.matchAllQuery());
            }
            
            sourceBuilder.sort("_id", SortOrder.ASC);
            sourceBuilder.size(paginationConfig.getPageSize());
            sourceBuilder.trackTotalHits(false); // ID分页不需要总数
            
            request.source(sourceBuilder);
            
            // 执行查询
            SearchResponse response = executeWithCircuitBreaker(sourceClient, request);
            SearchHit[] hits = response.getHits().getHits();
            
            if (hits.length == 0) {
                break;
            }
            
            // 处理当前页数据
            callback.processPage(hits, pageNumber, lastId != null ? lastId : "start");
            result.addProcessedCount(hits.length);
            
            // 更新游标
            lastId = hits[hits.length - 1].getId();
            
            // 日志记录
            if (pageNumber % 100 == 0) {
                log.info("ID分页进度 - 页数: {}, 已处理: {} 文档, 当前ID: {}", 
                    pageNumber, result.getProcessedCount(), lastId);
            }
            
            // 内存压力控制
            controlMemoryPressure();
            
            // 如果当前页不满，说明已经到末尾
            if (hits.length < paginationConfig.getPageSize()) {
                break;
            }
        }
        
        result.setTotalPages(pageNumber);
    }
    
    /**
     * 混合分页策略 - 时间+ID双重排序
     */
    private void paginateByHybrid(
            RestHighLevelClient sourceClient,
            MigrationConfig config,
            PaginationCallback callback,
            PaginationConfig paginationConfig,
            PaginationResult result) throws Exception {
        
        // 首先尝试时间分页，如果没有时间字段则回退到ID分页
        String timeField = paginationConfig.getTimeField() != null ? 
            paginationConfig.getTimeField() : config.getTimestampField();
        
        if (timeField != null && hasTimeField(sourceClient, config.getSourceIndex(), timeField)) {
            log.info("混合策略：使用时间分页");
            paginateByTime(sourceClient, config, callback, paginationConfig, result);
        } else {
            log.info("混合策略：回退到ID分页");
            paginateById(sourceClient, config, callback, paginationConfig, result);
        }
    }
    
    /**
     * 检查索引是否有指定的时间字段
     */
    private boolean hasTimeField(RestHighLevelClient client, String indexName, String timeField) {
        try {
            SearchRequest request = new SearchRequest(indexName);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.existsQuery(timeField));
            sourceBuilder.size(1);
            request.source(sourceBuilder);
            
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            return response.getHits().getTotalHits().value > 0;
        } catch (Exception e) {
            log.warn("检查时间字段失败: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * 获取数据的时间范围
     */
    private TimeRange getTimeRange(RestHighLevelClient client, String indexName, String timeField) throws Exception {
        // 获取最小时间
        SearchRequest minRequest = new SearchRequest(indexName);
        SearchSourceBuilder minSource = new SearchSourceBuilder();
        minSource.query(QueryBuilders.existsQuery(timeField));
        minSource.sort(timeField, SortOrder.ASC);
        minSource.size(1);
        minRequest.source(minSource);
        
        SearchResponse minResponse = client.search(minRequest, RequestOptions.DEFAULT);
        
        // 获取最大时间
        SearchRequest maxRequest = new SearchRequest(indexName);
        SearchSourceBuilder maxSource = new SearchSourceBuilder();
        maxSource.query(QueryBuilders.existsQuery(timeField));
        maxSource.sort(timeField, SortOrder.DESC);
        maxSource.size(1);
        maxRequest.source(maxSource);
        
        SearchResponse maxResponse = client.search(maxRequest, RequestOptions.DEFAULT);
        
        LocalDateTime startTime = LocalDateTime.MIN;
        LocalDateTime endTime = LocalDateTime.MAX;
        
        if (minResponse.getHits().getHits().length > 0) {
            Object minValue = minResponse.getHits().getHits()[0].getSourceAsMap().get(timeField);
            if (minValue != null) {
                startTime = LocalDateTime.parse(minValue.toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            }
        }
        
        if (maxResponse.getHits().getHits().length > 0) {
            Object maxValue = maxResponse.getHits().getHits()[0].getSourceAsMap().get(timeField);
            if (maxValue != null) {
                endTime = LocalDateTime.parse(maxValue.toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            }
        }
        
        return new TimeRange(startTime, endTime);
    }
    
    /**
     * 带熔断器的ES查询执行
     */
    private SearchResponse executeWithCircuitBreaker(RestHighLevelClient client, SearchRequest request) {
        return circuitBreakerManager.executeWithBreaker(
            "pagination_search",
            () -> {
                try {
                    return client.search(request, RequestOptions.DEFAULT);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            },
            () -> {
                log.error("分页查询熔断，无法提供降级服务");
                throw new RuntimeException("ES服务不可用，分页查询被熔断");
            }
        );
    }
    
    /**
     * 内存压力控制
     */
    private void controlMemoryPressure() throws InterruptedException {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long maxMemory = runtime.maxMemory();
        
        double memoryPressure = (double) (totalMemory - freeMemory) / maxMemory;
        
        if (memoryPressure > 0.9) {
            log.warn("内存压力过高 {:.1f}%，触发GC并暂停", memoryPressure * 100);
            System.gc();
            Thread.sleep(2000);
        } else if (memoryPressure > 0.8) {
            log.debug("内存压力较高 {:.1f}%，适当延迟", memoryPressure * 100);
            Thread.sleep(100);
        }
    }
    
    /**
     * 分页回调接口
     */
    public interface PaginationCallback {
        void processPage(SearchHit[] hits, long pageNumber, String pageKey) throws Exception;
    }
    
    /**
     * 分页配置
     */
    @Data
    public static class PaginationConfig {
        private PaginationStrategy strategy = PaginationStrategy.HYBRID;
        private int pageSize = DEFAULT_PAGE_SIZE;
        private String timeField;
        private int timeWindowMinutes = 10; // 时间窗口大小（分钟）
        
        public PaginationConfig strategy(PaginationStrategy strategy) {
            this.strategy = strategy;
            return this;
        }
        
        public PaginationConfig pageSize(int pageSize) {
            this.pageSize = Math.max(MIN_PAGE_SIZE, Math.min(pageSize, MAX_PAGE_SIZE));
            return this;
        }
        
        public PaginationConfig timeField(String timeField) {
            this.timeField = timeField;
            return this;
        }
        
        public PaginationConfig timeWindowMinutes(int minutes) {
            this.timeWindowMinutes = Math.max(1, minutes);
            return this;
        }
    }
    
    /**
     * 分页策略枚举
     */
    public enum PaginationStrategy {
        TIME_BASED,  // 基于时间的分页
        ID_BASED,    // 基于ID的分页
        HYBRID       // 混合策略
    }
    
    /**
     * 分页结果
     */
    @Data
    public static class PaginationResult {
        private long totalPages;
        private final AtomicLong processedCount = new AtomicLong(0);
        private final AtomicLong successCount = new AtomicLong(0);
        private final AtomicLong failedCount = new AtomicLong(0);
        private String errorMessage;
        private long totalTimeMs;
        
        public void addProcessedCount(long count) {
            processedCount.addAndGet(count);
        }
        
        public void addSuccessCount(long count) {
            successCount.addAndGet(count);
        }
        
        public void addFailedCount(long count) {
            failedCount.addAndGet(count);
        }
        
        public long getProcessedCount() { return processedCount.get(); }
        public long getSuccessCount() { return successCount.get(); }
        public long getFailedCount() { return failedCount.get(); }
    }
    
    /**
     * 时间范围
     */
    @Data
    private static class TimeRange {
        private final LocalDateTime startTime;
        private final LocalDateTime endTime;
        
        public TimeRange(LocalDateTime startTime, LocalDateTime endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }
}