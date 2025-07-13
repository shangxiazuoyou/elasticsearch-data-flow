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
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    
    private static final String SCROLL_ID_TIMEOUT = "5m";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    
    @Override
    public MigrationResult fullMigration(MigrationConfig config) {
        log.info("开始全量数据迁移，源索引: {}, 目标索引: {}, 响应缓冲区: {}MB",
                config.getSourceIndex(), config.getTargetIndex(),
                elasticsearchConfig.getResponseBufferLimit() / 1024 / 1024);
        
        MigrationResult result = new MigrationResult();
        
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
            
            // 执行迁移
            if (config.getThreadCount() > 1) {
                executeParallelMigration(config, result);
            } else {
                executeSingleThreadMigration(config, result);
            }
            
        } catch (Exception e) {
            log.error("全量迁移失败", e);
            result.setErrorMessage("迁移失败: " + e.getMessage());
        }
        
        result.finish();
        log.info("全量迁移完成，总数: {}, 成功: {}, 失败: {}, 跳过: {}, 耗时: {}ms", 
                result.getTotalCount(), result.getSuccessCount(), 
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
     * 执行单线程迁移
     */
    private void executeSingleThreadMigration(MigrationConfig config, MigrationResult result) throws IOException {
        SearchRequest searchRequest = new SearchRequest(config.getSourceIndex());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(config.getBatchSize());
        // 确保能获取准确的文档总数
        searchSourceBuilder.trackTotalHits(true);
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(config.getScrollTimeout()));

        // 使用查询辅助工具执行带重试机制的搜索
        log.info("开始执行全量迁移搜索，{}", esQueryHelper.getBufferConfigInfo());
        SearchResponse searchResponse = esQueryHelper.searchWithRetry(sourceClient, searchRequest, true, 3);
        String scrollId = searchResponse.getScrollId();

        SearchHit[] searchHits = searchResponse.getHits().getHits();

        while (searchHits != null && searchHits.length > 0) {
            processBatch(searchHits, config, result);

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
     * 处理批次数据
     */
    private void processBatch(SearchHit[] hits, MigrationConfig config, MigrationResult result) {
        BulkRequest bulkRequest = new BulkRequest();
        
        for (SearchHit hit : hits) {
            try {
                Map<String, Object> sourceData = hit.getSourceAsMap();
                Map<String, Object> targetData = transformDocument(sourceData, config);
                
                IndexRequest indexRequest = new IndexRequest(config.getTargetIndex())
                        .id(hit.getId())
                        .source(targetData, XContentType.JSON);
                
                bulkRequest.add(indexRequest);
                
            } catch (Exception e) {
                log.error("处理文档失败，ID: {}, 错误: {}", hit.getId(), e.getMessage());
                result.setFailedCount(result.getFailedCount() + 1);
            }
        }
        
        if (bulkRequest.numberOfActions() > 0) {
            try {
                BulkResponse bulkResponse = targetClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                if (bulkResponse.hasFailures()) {
                    log.warn("批量写入部分失败: {}", bulkResponse.buildFailureMessage());
                    result.setFailedCount(result.getFailedCount() + bulkResponse.getItems().length);
                } else {
                    result.setSuccessCount(result.getSuccessCount() + bulkResponse.getItems().length);
                }
            } catch (IOException e) {
                log.error("批量写入失败", e);
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
        ExecutorService executor = Executors.newFixedThreadPool(config.getThreadCount());
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // 这里可以实现更复杂的并行迁移逻辑
        // 为简化示例，暂时使用单线程迁移
        executeSingleThreadMigration(config, result);

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("并行迁移被中断", e);
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
}
