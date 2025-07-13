package com.everflowx.esmigration.controller;

import com.everflowx.esmigration.config.ElasticsearchConfig;
import com.everflowx.esmigration.exception.GlobalExceptionHandler;
import com.everflowx.esmigration.manager.MigrationTaskManager;
import com.everflowx.esmigration.service.EsMigrationService;
import com.everflowx.esmigration.service.IndexSyncService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
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
    
    @Autowired
    private GlobalExceptionHandler globalExceptionHandler;
    
    @Autowired
    private MigrationTaskManager taskManager;
    
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
    
    @ApiOperation("异常统计信息")
    @GetMapping("/exceptions")
    public Map<String, Object> getExceptionStats() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            Map<String, Object> stats = globalExceptionHandler.getExceptionStats();
            result.put("success", true);
            result.put("exceptionStats", stats);
            result.put("timestamp", System.currentTimeMillis());
            
            // 计算异常比率
            long total = (Long) stats.get("totalExceptions");
            if (total > 0) {
                Map<String, String> rates = new HashMap<>();
                rates.put("businessRate", String.format("%.1f%%", (Long) stats.get("businessExceptions") * 100.0 / total));
                rates.put("systemRate", String.format("%.1f%%", (Long) stats.get("systemExceptions") * 100.0 / total));
                rates.put("networkRate", String.format("%.1f%%", (Long) stats.get("networkExceptions") * 100.0 / total));
                rates.put("validationRate", String.format("%.1f%%", (Long) stats.get("validationExceptions") * 100.0 / total));
                result.put("exceptionRates", rates);
            }
            
        } catch (Exception e) {
            log.error("获取异常统计信息失败", e);
            result.put("success", false);
            result.put("message", "获取异常统计信息失败: " + e.getMessage());
            result.put("timestamp", System.currentTimeMillis());
        }
        
        return result;
    }
    
    // ==================== 标准健康检查端点 ====================
    
    /**
     * 标准健康检查端点 (符合Spring Boot Actuator规范)
     */
    @ApiOperation("标准健康检查")
    @GetMapping("/actuator")
    public ResponseEntity<Map<String, Object>> actuatorHealth() {
        Map<String, Object> health = new HashMap<>();
        String overallStatus = "UP";
        
        try {
            health.put("timestamp", System.currentTimeMillis());
            health.put("service", "ES Migration Monitor");
            health.put("version", "1.0.0");
            
            // 检查应用运行状态
            health.put("app", checkApplicationHealth());
            
            // 检查Elasticsearch连接
            Map<String, Object> esHealth = checkElasticsearchHealth();
            health.put("elasticsearch", esHealth);
            if ("DOWN".equals(esHealth.get("status"))) {
                overallStatus = "DOWN";
            }
            
            // 检查任务管理器状态
            Map<String, Object> taskHealth = checkTaskManagerHealth();
            health.put("tasks", taskHealth);
            if ("WARNING".equals(taskHealth.get("status"))) {
                overallStatus = "WARNING";
            }
            
            // 检查系统资源
            Map<String, Object> systemHealth = checkSystemHealth();
            health.put("system", systemHealth);
            if ("WARNING".equals(systemHealth.get("status"))) {
                overallStatus = "WARNING";
            }
            
            health.put("status", overallStatus);
            
            // 根据状态返回相应的HTTP状态码
            if ("DOWN".equals(overallStatus)) {
                return ResponseEntity.status(503).body(health);
            } else if ("WARNING".equals(overallStatus)) {
                return ResponseEntity.status(200).body(health);
            } else {
                return ResponseEntity.ok(health);
            }
            
        } catch (Exception e) {
            health.put("status", "DOWN");
            health.put("error", e.getMessage());
            health.put("timestamp", System.currentTimeMillis());
            return ResponseEntity.status(503).body(health);
        }
    }

    /**
     * 就绪检查
     */
    @ApiOperation("就绪检查")
    @GetMapping("/ready")
    public ResponseEntity<Map<String, Object>> readiness() {
        Map<String, Object> ready = new HashMap<>();
        boolean isReady = true;
        
        try {
            // 检查ES连接
            if (!isElasticsearchConnected()) {
                isReady = false;
                ready.put("elasticsearch", "NOT_READY");
            } else {
                ready.put("elasticsearch", "READY");
            }
            
            // 检查任务管理器
            if (taskManager == null) {
                isReady = false;
                ready.put("taskManager", "NOT_READY");
            } else {
                ready.put("taskManager", "READY");
            }
            
            ready.put("status", isReady ? "READY" : "NOT_READY");
            ready.put("timestamp", System.currentTimeMillis());
            
            return isReady ? ResponseEntity.ok(ready) : ResponseEntity.status(503).body(ready);
            
        } catch (Exception e) {
            ready.put("status", "ERROR");
            ready.put("error", e.getMessage());
            return ResponseEntity.status(503).body(ready);
        }
    }

    /**
     * 存活检查
     */
    @ApiOperation("存活检查")
    @GetMapping("/live")
    public ResponseEntity<Map<String, Object>> liveness() {
        Map<String, Object> live = new HashMap<>();
        
        try {
            // 简单的存活检查
            live.put("status", "ALIVE");
            live.put("timestamp", System.currentTimeMillis());
            live.put("uptime", ManagementFactory.getRuntimeMXBean().getUptime());
            
            return ResponseEntity.ok(live);
            
        } catch (Exception e) {
            live.put("status", "DEAD");
            live.put("error", e.getMessage());
            return ResponseEntity.status(500).body(live);
        }
    }

    /**
     * 详细健康检查
     */
    @ApiOperation("详细健康检查")
    @GetMapping("/detailed")
    public ResponseEntity<Map<String, Object>> detailedHealth() {
        Map<String, Object> health = new HashMap<>();
        
        try {
            health.put("timestamp", System.currentTimeMillis());
            health.put("service", "ES Migration Monitor");
            
            // 应用信息
            health.put("application", getApplicationInfo());
            
            // JVM信息
            health.put("jvm", getJvmInfo());
            
            // 系统信息
            health.put("system", getSystemInfo());
            
            // ES连接详情
            health.put("elasticsearch", getElasticsearchDetails());
            
            // 任务管理详情
            health.put("taskManager", getTaskManagerDetails());
            
            health.put("status", "UP");
            return ResponseEntity.ok(health);
            
        } catch (Exception e) {
            health.put("status", "ERROR");
            health.put("error", e.getMessage());
            return ResponseEntity.status(500).body(health);
        }
    }

    // ==================== 私有检查方法 ====================

    private Map<String, Object> checkApplicationHealth() {
        Map<String, Object> app = new HashMap<>();
        app.put("status", "UP");
        app.put("uptime", ManagementFactory.getRuntimeMXBean().getUptime());
        return app;
    }

    private Map<String, Object> checkElasticsearchHealth() {
        Map<String, Object> es = new HashMap<>();
        
        try {
            // 检查源ES连接
            boolean sourceConnected = sourceClient.ping(RequestOptions.DEFAULT);
            es.put("sourceConnection", sourceConnected ? "UP" : "DOWN");
            
            // 检查目标ES连接
            boolean targetConnected = targetClient.ping(RequestOptions.DEFAULT);
            es.put("targetConnection", targetConnected ? "UP" : "DOWN");
            
            if (sourceConnected && targetConnected) {
                es.put("status", "UP");
            } else {
                es.put("status", "DOWN");
            }
            
        } catch (Exception e) {
            es.put("status", "DOWN");
            es.put("error", e.getMessage());
        }
        
        return es;
    }

    private Map<String, Object> checkTaskManagerHealth() {
        Map<String, Object> tasks = new HashMap<>();
        
        try {
            int totalTasks = taskManager.getAllTasks().size();
            int runningTasks = taskManager.getRunningTasks().size();
            
            tasks.put("totalTasks", totalTasks);
            tasks.put("runningTasks", runningTasks);
            
            // 检查任务队列是否过载
            if (runningTasks > 50) {
                tasks.put("status", "WARNING");
                tasks.put("message", "任务队列过载");
            } else {
                tasks.put("status", "UP");
            }
            
        } catch (Exception e) {
            tasks.put("status", "DOWN");
            tasks.put("error", e.getMessage());
        }
        
        return tasks;
    }

    private Map<String, Object> checkSystemHealth() {
        Map<String, Object> system = new HashMap<>();
        
        try {
            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            long heapUsed = memoryBean.getHeapMemoryUsage().getUsed();
            long heapMax = memoryBean.getHeapMemoryUsage().getMax();
            double heapUsagePercent = (double) heapUsed / heapMax * 100;
            
            system.put("heapUsagePercent", String.format("%.2f%%", heapUsagePercent));
            
            // 内存使用率检查
            if (heapUsagePercent > 85) {
                system.put("status", "WARNING");
                system.put("message", "内存使用率过高");
            } else {
                system.put("status", "UP");
            }
            
        } catch (Exception e) {
            system.put("status", "WARNING");
            system.put("error", e.getMessage());
        }
        
        return system;
    }

    private boolean isElasticsearchConnected() {
        try {
            return sourceClient.ping(RequestOptions.DEFAULT) &&
                   targetClient.ping(RequestOptions.DEFAULT);
        } catch (Exception e) {
            return false;
        }
    }

    // ==================== 详细信息方法 ====================

    private Map<String, Object> getApplicationInfo() {
        Map<String, Object> app = new HashMap<>();
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
        
        app.put("name", "ES Migration Monitor");
        app.put("version", "1.0.0");
        app.put("startTime", runtimeBean.getStartTime());
        app.put("uptime", runtimeBean.getUptime());
        app.put("jvmName", runtimeBean.getVmName());
        app.put("jvmVersion", runtimeBean.getVmVersion());
        
        return app;
    }

    private Map<String, Object> getJvmInfo() {
        Map<String, Object> jvm = new HashMap<>();
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        
        // 堆内存信息
        Map<String, Object> heap = new HashMap<>();
        heap.put("used", memoryBean.getHeapMemoryUsage().getUsed());
        heap.put("max", memoryBean.getHeapMemoryUsage().getMax());
        heap.put("committed", memoryBean.getHeapMemoryUsage().getCommitted());
        jvm.put("heap", heap);
        
        // 非堆内存信息
        Map<String, Object> nonHeap = new HashMap<>();
        nonHeap.put("used", memoryBean.getNonHeapMemoryUsage().getUsed());
        nonHeap.put("max", memoryBean.getNonHeapMemoryUsage().getMax());
        nonHeap.put("committed", memoryBean.getNonHeapMemoryUsage().getCommitted());
        jvm.put("nonHeap", nonHeap);
        
        return jvm;
    }

    private Map<String, Object> getSystemInfo() {
        Map<String, Object> system = new HashMap<>();
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        
        system.put("osName", osBean.getName());
        system.put("osVersion", osBean.getVersion());
        system.put("osArch", osBean.getArch());
        system.put("availableProcessors", osBean.getAvailableProcessors());
        
        return system;
    }

    private Map<String, Object> getElasticsearchDetails() {
        Map<String, Object> es = new HashMap<>();
        
        try {
            // 源ES连接状态
            boolean sourceConnected = sourceClient.ping(RequestOptions.DEFAULT);
            es.put("sourceConnection", sourceConnected ? "CONNECTED" : "DISCONNECTED");
            
            // 目标ES连接状态  
            boolean targetConnected = targetClient.ping(RequestOptions.DEFAULT);
            es.put("targetConnection", targetConnected ? "CONNECTED" : "DISCONNECTED");
            
            es.put("status", (sourceConnected && targetConnected) ? "HEALTHY" : "UNHEALTHY");
            
        } catch (Exception e) {
            es.put("status", "ERROR");
            es.put("error", e.getMessage());
        }
        
        return es;
    }

    private Map<String, Object> getTaskManagerDetails() {
        Map<String, Object> tasks = new HashMap<>();
        
        try {
            tasks.put("totalTasks", taskManager.getAllTasks().size());
            tasks.put("runningTasks", taskManager.getRunningTasks().size());
            tasks.put("status", "OPERATIONAL");
            
        } catch (Exception e) {
            tasks.put("status", "ERROR");
            tasks.put("error", e.getMessage());
        }
        
        return tasks;
    }
}