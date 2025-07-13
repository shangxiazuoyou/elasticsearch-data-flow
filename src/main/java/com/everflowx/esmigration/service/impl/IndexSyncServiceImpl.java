package com.everflowx.esmigration.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.everflowx.esmigration.config.ElasticsearchConfig;
import com.everflowx.esmigration.domain.IndexSyncConfig;
import com.everflowx.esmigration.domain.IndexSyncResult;
import com.everflowx.esmigration.domain.MigrationConfig;
import com.everflowx.esmigration.service.EsMigrationService;
import com.everflowx.esmigration.service.IndexSyncService;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 索引同步服务实现类（简化版）
 * 
 * @author everflowx
 */
@Slf4j
@Service
public class IndexSyncServiceImpl implements IndexSyncService {
    
    @Resource
    @Qualifier("sourceElasticsearchClient")
    private RestHighLevelClient sourceClient;
    
    @Resource
    @Qualifier("targetElasticsearchClient")
    private RestHighLevelClient targetClient;
    
    @Autowired
    private ElasticsearchConfig elasticsearchConfig;
    
    @Autowired
    private EsMigrationService esMigrationService;
    
    @Override
    public IndexSyncResult syncIndices(IndexSyncConfig config) {
        IndexSyncResult result = new IndexSyncResult();
        
        try {
            log.info("开始索引同步任务，配置: {}", JSON.toJSONString(config));
            
            // 验证连接
            if (config.getValidateConnection()) {
                if (!validateConnections()) {
                    result.addError("ES连接验证失败");
                    result.finish();
                    return result;
                }
            }
            
            // 获取要同步的索引列表
            List<String> indicesToSync = getFilteredIndices(config);
            
            if (indicesToSync.isEmpty()) {
                result.addWarning("没有找到需要同步的索引");
                result.finish();
                return result;
            }
            
            log.info("找到 {} 个索引需要同步: {}", indicesToSync.size(), indicesToSync);
            
            // 并行同步索引
            if (config.getThreadCount() > 1) {
                syncIndicesParallel(indicesToSync, config, result);
            } else {
                syncIndicesSequential(indicesToSync, config, result);
            }
            
        } catch (Exception e) {
            log.error("索引同步失败", e);
            result.addError("索引同步失败: " + e.getMessage());
        } finally {
            result.finish();
        }
        
        return result;
    }
    
    @Override
    public IndexSyncResult syncSingleIndex(String indexName, IndexSyncConfig config) {
        IndexSyncResult result = new IndexSyncResult();
        
        try {
            log.info("开始同步单个索引: {}", indexName);
            
            if (!shouldSyncIndex(indexName, config)) {
                result.addWarning("索引 " + indexName + " 被跳过");
                IndexSyncResult.IndexSyncDetail detail = new IndexSyncResult.IndexSyncDetail();
                detail.setIndexName(indexName);
                detail.setStatus("SKIPPED");
                result.addIndexDetail(indexName, detail);
                result.finish();
                return result;
            }
            
            IndexSyncResult.IndexSyncDetail detail = syncSingleIndexInternal(indexName, config);
            result.addIndexDetail(indexName, detail);
            
        } catch (Exception e) {
            log.error("同步索引 {} 失败", indexName, e);
            result.addError("同步索引 " + indexName + " 失败: " + e.getMessage());
        } finally {
            result.finish();
        }
        
        return result;
    }
    
    /**
     * 内部同步单个索引的方法
     */
    private IndexSyncResult.IndexSyncDetail syncSingleIndexInternal(String indexName, IndexSyncConfig config) {
        IndexSyncResult.IndexSyncDetail detail = new IndexSyncResult.IndexSyncDetail();
        detail.setIndexName(indexName);
        detail.setStartTime(new Date());
        
        try {
            log.info("开始同步索引: {}", indexName);
            
            // 1. 创建索引（包括设置、映射）
            if (createIndex(indexName, config)) {
                detail.setSettingsSynced(config.getSyncSettings());
                detail.setMappingsSynced(config.getSyncMappings());
                log.info("索引 {} 创建成功", indexName);
            } else {
                detail.setStatus("FAILED");
                detail.setErrorMessage("索引创建失败");
                return detail;
            }
            
            // 2. 同步数据
            if (config.getSyncData()) {
                long syncedCount = syncIndexData(indexName, config);
                detail.setDocumentCount(syncedCount);
                detail.setSuccessCount(syncedCount);
                detail.setDataSynced(true);
                log.info("索引 {} 数据同步完成，同步文档数: {}", indexName, syncedCount);
            }
            
            // 3. 验证同步结果
            if (config.getValidateData() && !validateIndexSync(indexName)) {
                detail.setStatus("FAILED");
                detail.setErrorMessage("数据验证失败");
                return detail;
            }
            
            detail.setStatus("SUCCESS");
            log.info("索引 {} 同步成功", indexName);
            
        } catch (Exception e) {
            log.error("同步索引 {} 失败", indexName, e);
            detail.setStatus("FAILED");
            detail.setErrorMessage(e.getMessage());
        } finally {
            detail.setEndTime(new Date());
            if (detail.getStartTime() != null) {
                detail.setDurationMs(detail.getEndTime().getTime() - detail.getStartTime().getTime());
            }
        }
        
        return detail;
    }
    
    /**
     * 顺序同步索引
     */
    private void syncIndicesSequential(List<String> indices, IndexSyncConfig config, IndexSyncResult result) {
        for (String indexName : indices) {
            try {
                IndexSyncResult.IndexSyncDetail detail = syncSingleIndexInternal(indexName, config);
                result.addIndexDetail(indexName, detail);
                
                if ("FAILED".equals(detail.getStatus())) {
                    result.addError("索引 " + indexName + " 同步失败: " + detail.getErrorMessage());
                }
                
            } catch (Exception e) {
                log.error("同步索引 {} 失败", indexName, e);
                result.addError("同步索引 " + indexName + " 失败: " + e.getMessage());
            }
        }
    }
    
    /**
     * 并行同步索引
     */
    private void syncIndicesParallel(List<String> indices, IndexSyncConfig config, IndexSyncResult result) {
        ExecutorService executor = Executors.newFixedThreadPool(config.getThreadCount());
        
        try {
            List<CompletableFuture<IndexSyncResult.IndexSyncDetail>> futures = indices.stream()
                .map(indexName -> CompletableFuture.supplyAsync(() -> {
                    try {
                        return syncSingleIndexInternal(indexName, config);
                    } catch (Exception e) {
                        log.error("并行同步索引 {} 失败", indexName, e);
                        IndexSyncResult.IndexSyncDetail detail = new IndexSyncResult.IndexSyncDetail();
                        detail.setIndexName(indexName);
                        detail.setStatus("FAILED");
                        detail.setErrorMessage(e.getMessage());
                        return detail;
                    }
                }, executor))
                .collect(Collectors.toList());
            
            // 等待所有任务完成
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            
            // 收集结果
            for (CompletableFuture<IndexSyncResult.IndexSyncDetail> future : futures) {
                IndexSyncResult.IndexSyncDetail detail = future.get();
                result.addIndexDetail(detail.getIndexName(), detail);
                
                if ("FAILED".equals(detail.getStatus())) {
                    result.addError("索引 " + detail.getIndexName() + " 同步失败: " + detail.getErrorMessage());
                }
            }
            
        } catch (Exception e) {
            log.error("并行同步索引失败", e);
            result.addError("并行同步失败: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }
    
    @Override
    public List<String> getAllIndices(boolean isTarget) {
        try {
            RestHighLevelClient client = isTarget ? targetClient : sourceClient;
            GetIndexRequest request = new GetIndexRequest();
            request.indices("*");
            GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
            return Arrays.asList(response.getIndices());
        } catch (IOException e) {
            log.error("获取索引列表失败", e);
            return new ArrayList<>();
        }
    }
    
    @Override
    public List<String> getFilteredIndices(IndexSyncConfig config) {
        List<String> allIndices;
        
        // 如果指定了索引名称，直接使用
        if (!CollectionUtils.isEmpty(config.getIndexNames())) {
            allIndices = config.getIndexNames();
        } else {
            // 否则获取所有索引
            allIndices = getAllIndices(false);
        }
        
        return allIndices.stream()
            .filter(indexName -> shouldSyncIndex(indexName, config))
            .collect(Collectors.toList());
    }
    
    @Override
    public boolean shouldSyncIndex(String indexName, IndexSyncConfig config) {
        // 跳过系统索引
        if (config.getSkipSystemIndices() && indexName.startsWith(".")) {
            log.debug("跳过系统索引: {}", indexName);
            return false;
        }
        
        // 检查排除模式
        if (!CollectionUtils.isEmpty(config.getExcludePatterns())) {
            for (String pattern : config.getExcludePatterns()) {
                if (matchesPattern(indexName, pattern)) {
                    log.debug("索引 {} 匹配排除模式 {}", indexName, pattern);
                    return false;
                }
            }
        }
        
        // 检查包含模式
        if (!CollectionUtils.isEmpty(config.getIncludePatterns())) {
            boolean matches = false;
            for (String pattern : config.getIncludePatterns()) {
                if (matchesPattern(indexName, pattern)) {
                    matches = true;
                    break;
                }
            }
            if (!matches) {
                log.debug("索引 {} 不匹配任何包含模式", indexName);
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 检查索引名是否匹配模式（支持通配符）
     */
    private boolean matchesPattern(String indexName, String pattern) {
        String regex = pattern.replace("*", ".*").replace("?", ".");
        return Pattern.matches(regex, indexName);
    }
    
    /**
     * 验证ES连接
     */
    private boolean validateConnections() {
        try {
            // 验证源ES连接
            sourceClient.ping(RequestOptions.DEFAULT);
            log.info("源ES连接验证成功");
            
            // 验证目标ES连接
            targetClient.ping(RequestOptions.DEFAULT);
            log.info("目标ES连接验证成功");
            
            return true;
        } catch (Exception e) {
            log.error("ES连接验证失败", e);
            return false;
        }
    }

    @Override
    public boolean createIndex(String indexName, IndexSyncConfig config) {
        try {
            // 检查目标索引是否已存在
            GetIndexRequest existsRequest = new GetIndexRequest();
            existsRequest.indices(indexName);
            boolean targetExists = targetClient.indices().exists(existsRequest, RequestOptions.DEFAULT);

            if (targetExists) {
                if (config.getForceRecreate()) {
                    log.info("强制重新创建索引: {}", indexName);
                    // 删除现有索引
                    DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indexName);
                    targetClient.indices().delete(deleteRequest, RequestOptions.DEFAULT);
                    log.info("已删除现有索引: {}", indexName);
                } else if (!config.getOverwriteExisting()) {
                    log.info("索引 {} 已存在，跳过创建", indexName);
                    return true;
                }
            }

            // 获取源索引信息
            GetIndexRequest getRequest = new GetIndexRequest();
            getRequest.indices(indexName);
            GetIndexResponse getResponse = sourceClient.indices().get(getRequest, RequestOptions.DEFAULT);

            // 创建索引请求
            CreateIndexRequest createRequest = new CreateIndexRequest(indexName);

            // 同步设置
            if (config.getSyncSettings()) {
                Settings sourceSettings = getResponse.getSettings().get(indexName);
                if (sourceSettings != null) {
                    // 过滤掉不能设置的系统设置
                    Settings.Builder settingsBuilder = Settings.builder();
                    sourceSettings.keySet().forEach(key -> {
                        if (!isSystemSetting(key)) {
                            settingsBuilder.put(key, sourceSettings.get(key));
                        }
                    });
                    createRequest.settings(settingsBuilder);
                    log.debug("已设置索引 {} 的settings", indexName);
                }
            }

            // 同步映射（简化版本）
            if (config.getSyncMappings()) {
                try {
                    String mappingJson = getIndexMappings(indexName, false);
                    if (!"{}".equals(mappingJson)) {
                        createRequest.mapping(mappingJson, XContentType.JSON);
                        log.debug("已设置索引 {} 的mappings", indexName);
                    }
                } catch (Exception e) {
                    log.warn("获取索引 {} 映射失败，跳过映射同步: {}", indexName, e.getMessage());
                }
            }

            // 创建索引
            CreateIndexResponse createResponse = targetClient.indices().create(createRequest, RequestOptions.DEFAULT);

            if (createResponse.isAcknowledged()) {
                log.info("索引 {} 创建成功", indexName);
                return true;
            } else {
                log.error("索引 {} 创建失败", indexName);
                return false;
            }

        } catch (Exception e) {
            log.error("创建索引 {} 失败", indexName, e);
            return false;
        }
    }

    /**
     * 检查是否为系统设置
     */
    private boolean isSystemSetting(String key) {
        return key.startsWith("index.uuid") ||
               key.startsWith("index.creation_date") ||
               key.startsWith("index.provided_name") ||
               key.startsWith("index.version");
    }

    @Override
    public long syncIndexData(String indexName, IndexSyncConfig config) {
        try {
            // 构建迁移配置
            MigrationConfig migrationConfig = new MigrationConfig();
            migrationConfig.setSourceIndex(indexName);
            migrationConfig.setTargetIndex(indexName);
            migrationConfig.setBatchSize(config.getBatchSize());
            migrationConfig.setScrollTimeout(config.getScrollTimeout());
            migrationConfig.setThreadCount(config.getThreadCount());
            migrationConfig.setOverwriteExisting(config.getOverwriteExisting());
            migrationConfig.setNewFields(config.getNewFields());
            migrationConfig.setCaseInsensitiveFields(config.getCaseInsensitiveFields());
            migrationConfig.setFieldMapping(config.getFieldMapping());

            // 执行数据迁移
            EsMigrationService.MigrationResult result;
            if (config.getIncrementalMode()) {
                migrationConfig.setTimestampField(config.getTimestampField());
                Date lastSyncTime = esMigrationService.getLastSyncTime(indexName, config.getTimestampField());
                result = esMigrationService.incrementalMigration(migrationConfig, lastSyncTime);
            } else {
                result = esMigrationService.fullMigration(migrationConfig);
            }

            if (result.isSuccess()) {
                log.info("索引 {} 数据同步成功，处理文档数: {}", indexName, result.getSuccessCount());
                return result.getSuccessCount();
            } else {
                log.error("索引 {} 数据同步失败: {}", indexName, result.getErrorMessage());
                return 0;
            }

        } catch (Exception e) {
            log.error("同步索引 {} 数据失败", indexName, e);
            return 0;
        }
    }

    @Override
    public boolean validateIndexSync(String indexName) {
        try {
            long sourceCount = esMigrationService.getDocumentCount(indexName, false);
            long targetCount = esMigrationService.getDocumentCount(indexName, true);

            if (sourceCount == targetCount) {
                log.info("索引 {} 验证成功，文档数量一致: {}", indexName, sourceCount);
                return true;
            } else {
                log.warn("索引 {} 验证失败，源文档数: {}, 目标文档数: {}", indexName, sourceCount, targetCount);
                return false;
            }
        } catch (Exception e) {
            log.error("验证索引 {} 失败", indexName, e);
            return false;
        }
    }

    @Override
    public String getIndexSettings(String indexName, boolean isTarget) {
        try {
            RestHighLevelClient client = isTarget ? targetClient : sourceClient;
            GetIndexRequest request = new GetIndexRequest();
            request.indices(indexName);
            GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
            Settings settings = response.getSettings().get(indexName);

            if (settings != null) {
                // 将Settings转换为JSON格式
                JSONObject settingsJson = new JSONObject();

                // 遍历所有设置项
                for (String key : settings.keySet()) {
                    String value = settings.get(key);
                    // 处理嵌套的设置项
                    setNestedValue(settingsJson, key, value);
                }

                return settingsJson.toJSONString();
            }

            return "{}";
        } catch (Exception e) {
            log.error("获取索引 {} 设置失败", indexName, e);
            return "{}";
        }
    }

    /**
     * 设置嵌套的JSON值
     * 例如：index.number_of_shards -> {"index": {"number_of_shards": "1"}}
     */
    private void setNestedValue(JSONObject json, String key, String value) {
        String[] parts = key.split("\\.");
        JSONObject current = json;

        // 遍历到倒数第二层
        for (int i = 0; i < parts.length - 1; i++) {
            String part = parts[i];
            if (!current.containsKey(part)) {
                current.put(part, new JSONObject());
            }
            current = current.getJSONObject(part);
        }

        // 设置最后一层的值
        String lastKey = parts[parts.length - 1];

        // 尝试转换为合适的数据类型
        Object convertedValue = convertValue(value);
        current.put(lastKey, convertedValue);
    }

    /**
     * 转换值为合适的数据类型
     */
    private Object convertValue(String value) {
        if (value == null) {
            return null;
        }

        // 尝试转换为布尔值
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }

        // 尝试转换为数字
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            } else {
                return Long.parseLong(value);
            }
        } catch (NumberFormatException e) {
            // 不是数字，保持字符串
        }

        return value;
    }

    @Override
    public String getIndexMappings(String indexName, boolean isTarget) {
        try {
            RestHighLevelClient client = isTarget ? targetClient : sourceClient;
            GetIndexRequest request = new GetIndexRequest();
            request.indices(indexName);
            GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);

            // 获取映射信息并格式化为JSON
            try {
                Object mappingObj = response.getMappings().get(indexName);
                if (mappingObj != null) {
                    String mappingStr = mappingObj.toString();

                    // 尝试解析为JSON对象以确保格式正确
                    try {
                        JSONObject mappingJson = JSON.parseObject(mappingStr);
                        return mappingJson.toJSONString();
                    } catch (Exception parseEx) {
                        log.debug("映射字符串解析为JSON失败，返回原始字符串: {}", parseEx.getMessage());
                        return mappingStr;
                    }
                }
            } catch (Exception e) {
                log.debug("获取映射详情失败，返回空对象: {}", e.getMessage());
            }
            return "{}";
        } catch (Exception e) {
            log.error("获取索引 {} 映射失败", indexName, e);
            return "{}";
        }
    }

    @Override
    public String getIndexAliases(String indexName, boolean isTarget) {
        try {
            // 简化版本，暂时返回空对象
            log.debug("获取索引 {} 别名信息（简化版本）", indexName);
            return "{}";
        } catch (Exception e) {
            log.error("获取索引 {} 别名失败", indexName, e);
            return "{}";
        }
    }
}
