package com.everflowx.esmigration.controller;

import com.everflowx.esmigration.config.ElasticsearchConfig;
import com.everflowx.esmigration.service.EsMigrationService;
import com.everflowx.esmigration.service.IndexSyncService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 健康检查和服务状态控制器
 * 
 * @author everflowx
 */
@Api(tags = "系统健康检查")
@RestController
@RequestMapping("/api/health")
@Slf4j
public class HealthController {
    
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
    
    @Autowired
    private IndexSyncService indexSyncService;
    
    @ApiOperation("系统健康检查")
    @GetMapping("/check")
    public Map<String, Object> healthCheck() {
        Map<String, Object> result = new HashMap<>();
        
        // 检查应用状态
        result.put("application", "UP");
        result.put("timestamp", System.currentTimeMillis());
        
        // 检查ES连接
        Map<String, Object> esStatus = new HashMap<>();
        
        // 检查源ES
        try {
            boolean sourceConnected = sourceClient.ping(RequestOptions.DEFAULT);
            esStatus.put("sourceES", sourceConnected ? "UP" : "DOWN");
        } catch (Exception e) {
            esStatus.put("sourceES", "DOWN");
            esStatus.put("sourceESError", e.getMessage());
        }
        
        // 检查目标ES
        try {
            boolean targetConnected = targetClient.ping(RequestOptions.DEFAULT);
            esStatus.put("targetES", targetConnected ? "UP" : "DOWN");
        } catch (Exception e) {
            esStatus.put("targetES", "DOWN");
            esStatus.put("targetESError", e.getMessage());
        }
        
        result.put("elasticsearch", esStatus);
        
        // 检查服务状态
        Map<String, Object> services = new HashMap<>();
        services.put("migrationService", esMigrationService != null ? "UP" : "DOWN");
        services.put("indexSyncService", indexSyncService != null ? "UP" : "DOWN");
        result.put("services", services);
        
        // 检查配置状态
        Map<String, Object> config = new HashMap<>();
        config.put("bufferLimitMB", elasticsearchConfig.getResponseBufferLimit() / 1024 / 1024);
        config.put("bufferInitialKB", elasticsearchConfig.getResponseBufferInitial() / 1024);
        result.put("config", config);
        
        // 整体状态
        boolean allUp = "UP".equals(esStatus.get("sourceES")) && 
                       "UP".equals(esStatus.get("targetES")) &&
                       "UP".equals(services.get("migrationService")) &&
                       "UP".equals(services.get("indexSyncService"));
        
        result.put("status", allUp ? "UP" : "DOWN");
        
        return result;
    }
    
    @ApiOperation("ES连接详情")
    @GetMapping("/es-connections")
    public Map<String, Object> getEsConnections() {
        Map<String, Object> result = new HashMap<>();
        
        // 源ES连接信息
        Map<String, Object> sourceInfo = new HashMap<>();
        try {
            boolean connected = sourceClient.ping(RequestOptions.DEFAULT);
            sourceInfo.put("connected", connected);
            sourceInfo.put("status", connected ? "CONNECTED" : "DISCONNECTED");
        } catch (Exception e) {
            sourceInfo.put("connected", false);
            sourceInfo.put("status", "ERROR");
            sourceInfo.put("error", e.getMessage());
        }
        sourceInfo.put("type", "source");
        result.put("sourceES", sourceInfo);
        
        // 目标ES连接信息
        Map<String, Object> targetInfo = new HashMap<>();
        try {
            boolean connected = targetClient.ping(RequestOptions.DEFAULT);
            targetInfo.put("connected", connected);
            targetInfo.put("status", connected ? "CONNECTED" : "DISCONNECTED");
        } catch (Exception e) {
            targetInfo.put("connected", false);
            targetInfo.put("status", "ERROR");
            targetInfo.put("error", e.getMessage());
        }
        targetInfo.put("type", "target");
        result.put("targetES", targetInfo);
        
        return result;
    }
    
    @ApiOperation("服务状态详情")
    @GetMapping("/services")
    public Map<String, Object> getServiceStatus() {
        Map<String, Object> result = new HashMap<>();
        
        // 迁移服务状态
        Map<String, Object> migrationService = new HashMap<>();
        migrationService.put("available", esMigrationService != null);
        migrationService.put("className", esMigrationService != null ? esMigrationService.getClass().getSimpleName() : "N/A");
        result.put("migrationService", migrationService);
        
        // 索引同步服务状态
        Map<String, Object> syncService = new HashMap<>();
        syncService.put("available", indexSyncService != null);
        syncService.put("className", indexSyncService != null ? indexSyncService.getClass().getSimpleName() : "N/A");
        result.put("indexSyncService", syncService);
        
        return result;
    }
    
    @ApiOperation("快速功能测试")
    @GetMapping("/quick-test")
    public Map<String, Object> quickTest() {
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> tests = new HashMap<>();
        
        // 测试1：获取源ES索引列表
        try {
            List<String> sourceIndices = indexSyncService.getAllIndices(false);
            Map<String, Object> sourceTest = new HashMap<>();
            sourceTest.put("success", true);
            sourceTest.put("count", sourceIndices.size());
            sourceTest.put("sample", sourceIndices.size() > 0 ? sourceIndices.subList(0, Math.min(3, sourceIndices.size())) : "无索引");
            tests.put("getSourceIndices", sourceTest);
        } catch (Exception e) {
            Map<String, Object> sourceTest = new HashMap<>();
            sourceTest.put("success", false);
            sourceTest.put("error", e.getMessage());
            tests.put("getSourceIndices", sourceTest);
        }
        
        // 测试2：获取目标ES索引列表
        try {
            List<String> targetIndices = indexSyncService.getAllIndices(true);
            Map<String, Object> targetTest = new HashMap<>();
            targetTest.put("success", true);
            targetTest.put("count", targetIndices.size());
            targetTest.put("sample", targetIndices.size() > 0 ? targetIndices.subList(0, Math.min(3, targetIndices.size())) : "无索引");
            tests.put("getTargetIndices", targetTest);
        } catch (Exception e) {
            Map<String, Object> targetTest = new HashMap<>();
            targetTest.put("success", false);
            targetTest.put("error", e.getMessage());
            tests.put("getTargetIndices", targetTest);
        }
        
        // 测试3：配置管理功能
        try {
            Map<String, Object> bufferConfig = new HashMap<>();
            bufferConfig.put("bufferLimitMB", elasticsearchConfig.getResponseBufferLimit() / 1024 / 1024);
            bufferConfig.put("bufferInitialKB", elasticsearchConfig.getResponseBufferInitial() / 1024);
            
            Map<String, Object> configTest = new HashMap<>();
            configTest.put("success", true);
            configTest.put("bufferConfig", bufferConfig);
            tests.put("configManagement", configTest);
        } catch (Exception e) {
            Map<String, Object> configTest = new HashMap<>();
            configTest.put("success", false);
            configTest.put("error", e.getMessage());
            tests.put("configManagement", configTest);
        }
        
        result.put("tests", tests);
        
        // 计算测试通过率
        long successCount = tests.values().stream()
            .mapToLong(test -> {
                if (test instanceof Map) {
                    return Boolean.TRUE.equals(((Map<?, ?>) test).get("success")) ? 1 : 0;
                }
                return 0;
            })
            .sum();
        
        Map<String, Object> summary = new HashMap<>();
        summary.put("totalTests", tests.size());
        summary.put("passedTests", successCount);
        summary.put("passRate", String.format("%.1f%%", (double) successCount / tests.size() * 100));
        result.put("summary", summary);
        
        return result;
    }
    
    @ApiOperation("获取应用信息")
    @GetMapping("/info")
    public Map<String, Object> getAppInfo() {
        Map<String, Object> result = new HashMap<>();
        
        result.put("appName", "elasticsearch-data-flow");
        result.put("version", "1.0-SNAPSHOT");
        result.put("description", "Elasticsearch数据迁移和同步工具");
        result.put("author", "everflowx");
        
        // JVM信息
        Runtime runtime = Runtime.getRuntime();
        Map<String, Object> jvm = new HashMap<>();
        jvm.put("totalMemoryMB", runtime.totalMemory() / 1024 / 1024);
        jvm.put("freeMemoryMB", runtime.freeMemory() / 1024 / 1024);
        jvm.put("maxMemoryMB", runtime.maxMemory() / 1024 / 1024);
        jvm.put("usedMemoryMB", (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024);
        jvm.put("processors", runtime.availableProcessors());
        result.put("jvm", jvm);
        
        // 系统信息
        Map<String, Object> system = new HashMap<>();
        system.put("osName", System.getProperty("os.name"));
        system.put("osVersion", System.getProperty("os.version"));
        system.put("javaVersion", System.getProperty("java.version"));
        result.put("system", system);
        
        return result;
    }
}