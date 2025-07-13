package com.everflowx.esmigration.controller;

import com.everflowx.esmigration.domain.IndexSyncConfig;
import com.everflowx.esmigration.domain.MigrationConfig;
import com.everflowx.esmigration.domain.MigrationTask;
import com.everflowx.esmigration.domain.TaskStatus;
import com.everflowx.esmigration.exception.MigrationConfigException;
import com.everflowx.esmigration.manager.MigrationTaskManager;
import com.everflowx.esmigration.service.EsMigrationService;
import com.everflowx.esmigration.service.EnhancedMigrationService;
import com.everflowx.esmigration.service.IndexSyncService;
import com.everflowx.esmigration.util.ConfigValidator;
import com.everflowx.esmigration.websocket.MonitorWebSocketHandler;
import com.everflowx.esmigration.service.SystemStatisticsService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 监控面板控制器
 * 
 * @author everflowx
 */
@Api(tags = "实时监控面板")
@RestController
@RequestMapping("/api/monitor")
@Slf4j
public class MonitorController {
    
    @Autowired
    private MigrationTaskManager taskManager;
    
    @Autowired
    private EsMigrationService migrationService;
    
    @Autowired
    private EnhancedMigrationService enhancedMigrationService;
    
    @Autowired
    private IndexSyncService indexSyncService;
    
    @Autowired
    private ConfigValidator configValidator;
    
    @Autowired
    private MonitorWebSocketHandler webSocketHandler;
    
    @Autowired
    private SystemStatisticsService systemStatisticsService;
    
    @ApiOperation("获取所有任务列表")
    @GetMapping("/tasks")
    public Map<String, Object> getAllTasks() {
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("tasks", taskManager.getTaskSummaries());
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
    
    @ApiOperation("获取任务详情")
    @GetMapping("/tasks/{taskId}")
    public Map<String, Object> getTaskDetail(@PathVariable String taskId) {
        Map<String, Object> result = new HashMap<>();
        
        MigrationTask task = taskManager.getTask(taskId);
        if (task != null) {
            result.put("success", true);
            result.put("task", task);
        } else {
            result.put("success", false);
            result.put("message", "任务不存在: " + taskId);
        }
        
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
    
    @ApiOperation("根据状态获取任务")
    @GetMapping("/tasks/status/{status}")
    public Map<String, Object> getTasksByStatus(@PathVariable TaskStatus status) {
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("status", status);
        result.put("tasks", taskManager.getTasksByStatus(status));
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
    
    @ApiOperation("获取正在运行的任务")
    @GetMapping("/tasks/running")
    public Map<String, Object> getRunningTasks() {
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("tasks", taskManager.getRunningTasks());
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
    
    @ApiOperation("获取系统统计信息")
    @GetMapping("/stats")
    public Map<String, Object> getSystemStats() {
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("stats", taskManager.getSystemStats());
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
    
    @ApiOperation("获取增强版系统统计信息")
    @GetMapping("/stats/enhanced")
    public Map<String, Object> getEnhancedSystemStats() {
        Map<String, Object> result = new HashMap<>();
        try {
            Map<String, Object> taskStats = systemStatisticsService.getTaskStatistics();
            Map<String, Object> performanceStats = systemStatisticsService.getPerformanceStatistics();
            Map<String, Object> realtimeMetrics = systemStatisticsService.getRealtimeMetrics();
            
            result.put("success", true);
            result.put("taskStats", taskStats);
            result.put("performanceStats", performanceStats);
            result.put("realtimeMetrics", realtimeMetrics);
            result.put("timestamp", System.currentTimeMillis());
        } catch (Exception e) {
            log.error("获取增强版系统统计失败", e);
            result.put("success", false);
            result.put("message", "获取系统统计失败: " + e.getMessage());
        }
        return result;
    }
    
    @ApiOperation("获取完整系统统计报告")
    @GetMapping("/stats/comprehensive")
    public Map<String, Object> getComprehensiveStats() {
        Map<String, Object> result = new HashMap<>();
        try {
            Map<String, Object> comprehensiveStats = systemStatisticsService.getComprehensiveStats();
            result.put("success", true);
            result.put("stats", comprehensiveStats);
            result.put("timestamp", System.currentTimeMillis());
        } catch (Exception e) {
            log.error("获取完整系统统计失败", e);
            result.put("success", false);
            result.put("message", "获取完整系统统计失败: " + e.getMessage());
        }
        return result;
    }
    
    @ApiOperation("获取系统资源统计")
    @GetMapping("/stats/resources")
    public Map<String, Object> getSystemResourceStats() {
        Map<String, Object> result = new HashMap<>();
        try {
            Map<String, Object> resourceStats = systemStatisticsService.getSystemResourceStats();
            result.put("success", true);
            result.put("resourceStats", resourceStats);
            result.put("timestamp", System.currentTimeMillis());
        } catch (Exception e) {
            log.error("获取系统资源统计失败", e);
            result.put("success", false);
            result.put("message", "获取系统资源统计失败: " + e.getMessage());
        }
        return result;
    }
    
    @ApiOperation("获取性能统计")
    @GetMapping("/stats/performance")
    public Map<String, Object> getPerformanceStats() {
        Map<String, Object> result = new HashMap<>();
        try {
            Map<String, Object> performanceStats = systemStatisticsService.getPerformanceStatistics();
            result.put("success", true);
            result.put("performanceStats", performanceStats);
            result.put("timestamp", System.currentTimeMillis());
        } catch (Exception e) {
            log.error("获取性能统计失败", e);
            result.put("success", false);
            result.put("message", "获取性能统计失败: " + e.getMessage());
        }
        return result;
    }
    
    @ApiOperation("获取历史趋势数据")
    @GetMapping("/stats/trends")
    public Map<String, Object> getHistoricalTrends() {
        Map<String, Object> result = new HashMap<>();
        try {
            Map<String, Object> trendData = systemStatisticsService.getHistoricalTrends();
            result.put("success", true);
            result.put("trendData", trendData);
            result.put("timestamp", System.currentTimeMillis());
        } catch (Exception e) {
            log.error("获取历史趋势数据失败", e);
            result.put("success", false);
            result.put("message", "获取历史趋势数据失败: " + e.getMessage());
        }
        return result;
    }
    
    @ApiOperation("手动触发系统统计快照")
    @PostMapping("/stats/snapshot")
    public Map<String, Object> triggerStatisticsSnapshot() {
        Map<String, Object> result = new HashMap<>();
        try {
            systemStatisticsService.recordStatisticsSnapshot();
            result.put("success", true);
            result.put("message", "系统统计快照已记录");
            result.put("timestamp", System.currentTimeMillis());
        } catch (Exception e) {
            log.error("记录系统统计快照失败", e);
            result.put("success", false);
            result.put("message", "记录系统统计快照失败: " + e.getMessage());
        }
        return result;
    }
    
    @ApiOperation("启动全量数据迁移任务")
    @PostMapping("/tasks/migration/full")
    public Map<String, Object> startFullMigrationTask(@RequestBody MigrationConfig config) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            // 输入安全检查
            if (config == null) {
                throw new MigrationConfigException("INVALID_CONFIG", "迁移配置不能为空");
            }
            
            // 验证配置
            configValidator.validateMigrationConfig(config);
            
            // 安全检查：防止索引名注入
            validateIndexNameSecurity(config.getSourceIndex());
            validateIndexNameSecurity(config.getTargetIndex());
            
            // 创建任务
            String taskName = String.format("全量迁移: %s -> %s", config.getSourceIndex(), config.getTargetIndex());
            MigrationTask task = taskManager.createTask(taskName, "FULL_MIGRATION");
            task.setMigrationConfig(config);
            
            // 异步执行迁移
            executeFullMigrationAsync(task);
            
            result.put("success", true);
            result.put("taskId", task.getTaskId());
            result.put("message", "全量迁移任务已创建");
            
        } catch (Exception e) {
            log.error("创建全量迁移任务失败", e);
            result.put("success", false);
            result.put("message", "创建任务失败: " + e.getMessage());
        }
        
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
    
    @ApiOperation("启动增量数据迁移任务")
    @PostMapping("/tasks/migration/incremental")
    public Map<String, Object> startIncrementalMigrationTask(@RequestBody MigrationConfig config) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            // 验证配置
            configValidator.validateMigrationConfig(config);
            
            // 创建任务
            String taskName = String.format("增量迁移: %s -> %s", config.getSourceIndex(), config.getTargetIndex());
            MigrationTask task = taskManager.createTask(taskName, "INCREMENTAL_MIGRATION");
            task.setMigrationConfig(config);
            
            // 异步执行迁移
            executeIncrementalMigrationAsync(task);
            
            result.put("success", true);
            result.put("taskId", task.getTaskId());
            result.put("message", "增量迁移任务已创建");
            
        } catch (Exception e) {
            log.error("创建增量迁移任务失败", e);
            result.put("success", false);
            result.put("message", "创建任务失败: " + e.getMessage());
        }
        
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
    
    @ApiOperation("启动索引同步任务")
    @PostMapping("/tasks/sync")
    public Map<String, Object> startIndexSyncTask(@RequestBody IndexSyncConfig config) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            // 验证配置
            configValidator.validateIndexSyncConfig(config);
            
            // 创建任务
            String taskName = "索引同步任务";
            if (config.getIndexNames() != null && !config.getIndexNames().isEmpty()) {
                taskName = "索引同步: " + String.join(", ", config.getIndexNames());
            }
            
            MigrationTask task = taskManager.createTask(taskName, "INDEX_SYNC");
            task.setIndexSyncConfig(config);
            
            // 异步执行同步
            executeIndexSyncAsync(task);
            
            result.put("success", true);
            result.put("taskId", task.getTaskId());
            result.put("message", "索引同步任务已创建");
            
        } catch (Exception e) {
            log.error("创建索引同步任务失败", e);
            result.put("success", false);
            result.put("message", "创建任务失败: " + e.getMessage());
        }
        
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
    
    @ApiOperation("取消任务")
    @PostMapping("/tasks/{taskId}/cancel")
    public Map<String, Object> cancelTask(@PathVariable String taskId) {
        Map<String, Object> result = new HashMap<>();
        
        boolean success = taskManager.cancelTask(taskId);
        result.put("success", success);
        result.put("message", success ? "任务已取消" : "无法取消任务");
        result.put("timestamp", System.currentTimeMillis());
        
        return result;
    }
    
    @ApiOperation("暂停任务")
    @PostMapping("/tasks/{taskId}/pause")
    public Map<String, Object> pauseTask(@PathVariable String taskId) {
        Map<String, Object> result = new HashMap<>();
        
        boolean success = taskManager.pauseTask(taskId);
        result.put("success", success);
        result.put("message", success ? "任务已暂停" : "无法暂停任务");
        result.put("timestamp", System.currentTimeMillis());
        
        return result;
    }
    
    @ApiOperation("恢复任务")
    @PostMapping("/tasks/{taskId}/resume")
    public Map<String, Object> resumeTask(@PathVariable String taskId) {
        Map<String, Object> result = new HashMap<>();
        
        boolean success = taskManager.resumeTask(taskId);
        result.put("success", success);
        result.put("message", success ? "任务已恢复" : "无法恢复任务");
        result.put("timestamp", System.currentTimeMillis());
        
        return result;
    }
    
    @ApiOperation("删除任务")
    @DeleteMapping("/tasks/{taskId}")
    public Map<String, Object> deleteTask(@PathVariable String taskId) {
        Map<String, Object> result = new HashMap<>();
        
        boolean success = taskManager.deleteTask(taskId);
        result.put("success", success);
        result.put("message", success ? "任务已删除" : "无法删除任务");
        result.put("timestamp", System.currentTimeMillis());
        
        return result;
    }
    
    @ApiOperation("推送完整任务列表到WebSocket")
    @PostMapping("/broadcast/tasks")
    public Map<String, Object> broadcastTaskList() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            webSocketHandler.pushTaskList();
            result.put("success", true);
            result.put("message", "任务列表已推送");
        } catch (Exception e) {
            result.put("success", false);
            result.put("message", "推送失败: " + e.getMessage());
        }
        
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
    
    @ApiOperation("推送系统统计信息到WebSocket")
    @PostMapping("/broadcast/stats")
    public Map<String, Object> broadcastSystemStats() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            webSocketHandler.pushSystemStats();
            result.put("success", true);
            result.put("message", "系统统计信息已推送");
        } catch (Exception e) {
            result.put("success", false);
            result.put("message", "推送失败: " + e.getMessage());
        }
        
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
    
    @ApiOperation("推送增强版系统统计信息到WebSocket")
    @PostMapping("/broadcast/enhanced-stats")
    public Map<String, Object> broadcastEnhancedSystemStats() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            Map<String, Object> taskStats = systemStatisticsService.getTaskStatistics();
            Map<String, Object> performanceStats = systemStatisticsService.getPerformanceStatistics();
            Map<String, Object> realtimeMetrics = systemStatisticsService.getRealtimeMetrics();
            
            Map<String, Object> pushData = new HashMap<>();
            pushData.put("taskStats", taskStats);
            pushData.put("performanceStats", performanceStats);
            pushData.put("realtimeMetrics", realtimeMetrics);
            pushData.put("timestamp", System.currentTimeMillis());
            
            webSocketHandler.pushSystemStats(pushData);
            
            result.put("success", true);
            result.put("message", "增强版系统统计信息已推送");
        } catch (Exception e) {
            log.error("推送增强版系统统计失败", e);
            result.put("success", false);
            result.put("message", "推送失败: " + e.getMessage());
        }
        
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
    
    @ApiOperation("发送性能告警")
    @PostMapping("/alerts/performance")
    public Map<String, Object> sendPerformanceAlert(
            @RequestParam String alertType,
            @RequestParam String message,
            @RequestBody(required = false) Map<String, Object> metrics) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            webSocketHandler.pushPerformanceAlert(alertType, message, metrics);
            result.put("success", true);
            result.put("message", "性能告警已发送");
        } catch (Exception e) {
            log.error("发送性能告警失败", e);
            result.put("success", false);
            result.put("message", "发送告警失败: " + e.getMessage());
        }
        
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
    
    // ========== 安全验证方法 ==========
    
    /**
     * 验证索引名称安全性
     */
    private void validateIndexNameSecurity(String indexName) {
        if (indexName == null || indexName.trim().isEmpty()) {
            throw new MigrationConfigException("INVALID_INDEX_NAME", "索引名称不能为空");
        }
        
        // 长度检查
        if (indexName.length() > 255) {
            throw new MigrationConfigException("INVALID_INDEX_NAME", "索引名称长度不能超过255个字符");
        }
        
        // 字符安全检查
        String sanitized = indexName.replaceAll("[^a-zA-Z0-9_-]", "");
        if (!sanitized.equals(indexName)) {
            throw new MigrationConfigException("INVALID_INDEX_NAME", "索引名称包含非法字符，只允许字母、数字、下划线和连字符");
        }
        
        // 禁用系统索引
        if (indexName.startsWith(".") || indexName.startsWith("_")) {
            throw new MigrationConfigException("INVALID_INDEX_NAME", "不允许操作系统索引");
        }
        
        // 危险关键词检查
        String[] dangerousKeywords = {"delete", "drop", "truncate", "admin", "system"};
        String lowerIndexName = indexName.toLowerCase();
        for (String keyword : dangerousKeywords) {
            if (lowerIndexName.contains(keyword)) {
                log.warn("检测到潜在危险的索引名称: {}", indexName);
                // 这里不抛异常，只记录警告，但在生产环境中可能需要更严格的检查
            }
        }
    }
    
    /**
     * 验证批次大小安全性
     */
    private void validateBatchSizeSecurity(Integer batchSize) {
        if (batchSize == null || batchSize <= 0) {
            throw new MigrationConfigException("INVALID_BATCH_SIZE", "批次大小必须大于0");
        }
        
        if (batchSize > 10000) {
            throw new MigrationConfigException("INVALID_BATCH_SIZE", "批次大小不能超过10000，防止内存溢出");
        }
        
        if (batchSize < 100) {
            log.warn("批次大小过小可能影响性能: {}", batchSize);
        }
    }
    
    /**
     * 验证线程数安全性
     */
    private void validateThreadCountSecurity(Integer threadCount) {
        if (threadCount == null || threadCount <= 0) {
            throw new MigrationConfigException("INVALID_THREAD_COUNT", "线程数必须大于0");
        }
        
        if (threadCount > 10) {
            throw new MigrationConfigException("INVALID_THREAD_COUNT", "线程数不能超过10，防止系统过载");
        }
    }
    
    // ========== 异步任务执行方法 ==========
    
    @Async
    public CompletableFuture<Void> executeFullMigrationAsync(MigrationTask task) {
        return CompletableFuture.runAsync(() -> {
            try {
                taskManager.startTask(task.getTaskId());
                
                // 使用增强版迁移服务，支持进度回调
                MigrationConfig config = task.getMigrationConfig();
                EsMigrationService.MigrationResult result = enhancedMigrationService.fullMigrationWithCallback(task.getTaskId(), config);
                
                // 更新性能指标
                systemStatisticsService.updatePerformanceMetrics(
                    result.getSuccessCount(), 
                    task.getDurationMs()
                );
                
                // 模拟进度更新（实际项目中应该在迁移过程中实时更新）
                long totalDocs = result.getTotalCount() > 0 ? result.getTotalCount() : 10000; // 如果无法获取总数，使用默认值
                enhancedMigrationService.simulateProgressUpdate(task.getTaskId(), totalDocs);
                
                // 更新最终状态
                taskManager.updateTaskProgress(task.getTaskId(), 
                    result.getTotalCount(), 
                    result.getSuccessCount() + result.getFailedCount(),
                    result.getSuccessCount(), 
                    result.getFailedCount(), 
                    "迁移完成");
                
                taskManager.completeTask(task.getTaskId(), result.isSuccess(), result.getErrorMessage());
                
            } catch (Exception e) {
                log.error("执行全量迁移任务失败: {}", task.getTaskId(), e);
                taskManager.completeTask(task.getTaskId(), false, e.getMessage());
                webSocketHandler.pushErrorMessage(task.getTaskId(), e.getMessage());
            }
        });
    }
    
    @Async
    public CompletableFuture<Void> executeIncrementalMigrationAsync(MigrationTask task) {
        return CompletableFuture.runAsync(() -> {
            try {
                taskManager.startTask(task.getTaskId());
                taskManager.updateTaskProgress(task.getTaskId(), 0, 0, 0, 0, "开始增量迁移");
                
                // 使用增强版迁移服务执行增量迁移
                MigrationConfig config = task.getMigrationConfig();
                // 获取上次同步时间
                java.util.Date lastSyncTime = migrationService.getLastSyncTime(config.getTargetIndex(), config.getTimestampField());
                
                EsMigrationService.MigrationResult result = enhancedMigrationService.incrementalMigrationWithCallback(task.getTaskId(), config, lastSyncTime);
                
                // 更新性能指标
                systemStatisticsService.updatePerformanceMetrics(
                    result.getSuccessCount(), 
                    task.getDurationMs()
                );
                
                // 更新最终状态
                taskManager.updateTaskProgress(task.getTaskId(), 
                    result.getTotalCount(), 
                    result.getSuccessCount() + result.getFailedCount(),
                    result.getSuccessCount(), 
                    result.getFailedCount(), 
                    "增量迁移完成");
                
                taskManager.completeTask(task.getTaskId(), result.isSuccess(), result.getErrorMessage());
                
            } catch (Exception e) {
                log.error("执行增量迁移任务失败: {}", task.getTaskId(), e);
                taskManager.completeTask(task.getTaskId(), false, e.getMessage());
                webSocketHandler.pushErrorMessage(task.getTaskId(), e.getMessage());
            }
        });
    }
    
    @Async
    public CompletableFuture<Void> executeIndexSyncAsync(MigrationTask task) {
        return CompletableFuture.runAsync(() -> {
            try {
                taskManager.startTask(task.getTaskId());
                taskManager.updateTaskProgress(task.getTaskId(), 0, 0, 0, 0, "开始索引同步");
                
                // 执行索引同步
                IndexSyncConfig config = task.getIndexSyncConfig();
                com.everflowx.esmigration.domain.IndexSyncResult result = indexSyncService.syncIndices(config);
                
                // 更新性能指标
                systemStatisticsService.updatePerformanceMetrics(
                    result.getSuccessDocuments(), 
                    task.getDurationMs()
                );
                
                // 更新最终状态
                taskManager.updateTaskProgress(task.getTaskId(), 
                    result.getTotalDocuments(), 
                    result.getSuccessDocuments() + result.getFailedDocuments(),
                    result.getSuccessDocuments(), 
                    result.getFailedDocuments(), 
                    "索引同步完成");
                
                taskManager.completeTask(task.getTaskId(), result.getSuccess(), result.getErrorMessage());
                
            } catch (Exception e) {
                log.error("执行索引同步任务失败: {}", task.getTaskId(), e);
                taskManager.completeTask(task.getTaskId(), false, e.getMessage());
                webSocketHandler.pushErrorMessage(task.getTaskId(), e.getMessage());
            }
        });
    }
}