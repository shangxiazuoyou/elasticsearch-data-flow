package com.everflowx.esmigration.monitor;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 迁移监控器 - 实时监控迁移性能和状态
 * 
 * @author everflowx
 */
@Slf4j
@Component
public class MigrationMonitor {
    
    // 任务监控信息
    private final ConcurrentHashMap<String, TaskMetrics> taskMetricsMap = new ConcurrentHashMap<>();
    
    // 全局统计信息
    private final LongAdder totalDocumentsProcessed = new LongAdder();
    private final LongAdder totalDocumentsSucceeded = new LongAdder();
    private final LongAdder totalDocumentsFailed = new LongAdder();
    private final AtomicLong globalStartTime = new AtomicLong(0);
    
    /**
     * 开始监控任务
     */
    public void startTask(String taskId, String sourceIndex, String targetIndex, long totalDocuments) {
        TaskMetrics metrics = new TaskMetrics();
        metrics.setTaskId(taskId);
        metrics.setSourceIndex(sourceIndex);
        metrics.setTargetIndex(targetIndex);
        metrics.setTotalDocuments(totalDocuments);
        metrics.setStartTime(System.currentTimeMillis());
        metrics.setLastUpdateTime(System.currentTimeMillis());
        metrics.setStatus("RUNNING");
        
        taskMetricsMap.put(taskId, metrics);
        
        // 设置全局开始时间
        if (globalStartTime.compareAndSet(0, System.currentTimeMillis())) {
            log.info("开始监控迁移任务，任务ID: {}, 源索引: {}, 目标索引: {}, 总文档数: {}", 
                taskId, sourceIndex, targetIndex, totalDocuments);
        }
    }
    
    /**
     * 更新任务进度
     */
    public void updateProgress(String taskId, long processedCount, long successCount, long failedCount) {
        TaskMetrics metrics = taskMetricsMap.get(taskId);
        if (metrics != null) {
            long currentTime = System.currentTimeMillis();
            long lastProcessed = metrics.getProcessedDocuments();
            long lastUpdateTime = metrics.getLastUpdateTime();
            
            // 计算处理速度
            if (currentTime > lastUpdateTime && processedCount > lastProcessed) {
                long timeDiff = currentTime - lastUpdateTime;
                long countDiff = processedCount - lastProcessed;
                double currentSpeed = (double) countDiff / timeDiff * 1000; // docs/second
                
                metrics.setCurrentSpeed(currentSpeed);
                
                // 计算平均速度
                long totalTime = currentTime - metrics.getStartTime();
                if (totalTime > 0) {
                    double avgSpeed = (double) processedCount / totalTime * 1000;
                    metrics.setAverageSpeed(avgSpeed);
                }
                
                // 计算预计剩余时间
                if (currentSpeed > 0 && metrics.getTotalDocuments() > processedCount) {
                    long remaining = metrics.getTotalDocuments() - processedCount;
                    long eta = (long)(remaining / currentSpeed);
                    metrics.setEstimatedRemainingTime(eta);
                }
            }
            
            metrics.setProcessedDocuments(processedCount);
            metrics.setSuccessDocuments(successCount);
            metrics.setFailedDocuments(failedCount);
            metrics.setLastUpdateTime(currentTime);
            
            // 更新全局统计
            totalDocumentsProcessed.add(processedCount - lastProcessed);
            totalDocumentsSucceeded.add(successCount - metrics.getSuccessDocuments());
            totalDocumentsFailed.add(failedCount - metrics.getFailedDocuments());
        }
    }
    
    /**
     * 标记任务完成
     */
    public void completeTask(String taskId, String status) {
        TaskMetrics metrics = taskMetricsMap.get(taskId);
        if (metrics != null) {
            metrics.setStatus(status);
            metrics.setEndTime(System.currentTimeMillis());
            
            long duration = metrics.getEndTime() - metrics.getStartTime();
            double overallSpeed = duration > 0 ? (double) metrics.getProcessedDocuments() / duration * 1000 : 0;
            metrics.setOverallSpeed(overallSpeed);
            
            log.info("任务完成 - ID: {}, 状态: {}, 耗时: {}ms, 平均速度: {:.1f} docs/s, " +
                    "成功: {}, 失败: {}, 成功率: {:.2f}%", 
                    taskId, status, duration, overallSpeed, 
                    metrics.getSuccessDocuments(), metrics.getFailedDocuments(),
                    calculateSuccessRate(metrics.getSuccessDocuments(), metrics.getProcessedDocuments()));
        }
    }
    
    /**
     * 获取任务指标
     */
    public TaskMetrics getTaskMetrics(String taskId) {
        return taskMetricsMap.get(taskId);
    }
    
    /**
     * 获取全局统计
     */
    public GlobalMetrics getGlobalMetrics() {
        GlobalMetrics global = new GlobalMetrics();
        global.setTotalTasks(taskMetricsMap.size());
        global.setRunningTasks((int) taskMetricsMap.values().stream().filter(m -> "RUNNING".equals(m.getStatus())).count());
        global.setCompletedTasks((int) taskMetricsMap.values().stream().filter(m -> "COMPLETED".equals(m.getStatus())).count());
        global.setFailedTasks((int) taskMetricsMap.values().stream().filter(m -> "FAILED".equals(m.getStatus())).count());
        
        global.setTotalDocumentsProcessed(totalDocumentsProcessed.sum());
        global.setTotalDocumentsSucceeded(totalDocumentsSucceeded.sum());
        global.setTotalDocumentsFailed(totalDocumentsFailed.sum());
        
        if (globalStartTime.get() > 0) {
            long globalDuration = System.currentTimeMillis() - globalStartTime.get();
            double globalSpeed = globalDuration > 0 ? (double) global.getTotalDocumentsProcessed() / globalDuration * 1000 : 0;
            global.setOverallSpeed(globalSpeed);
            global.setDuration(globalDuration);
        }
        
        return global;
    }
    
    /**
     * 记录详细性能日志
     */
    public void logDetailedMetrics(String taskId) {
        TaskMetrics metrics = taskMetricsMap.get(taskId);
        if (metrics != null) {
            Runtime runtime = Runtime.getRuntime();
            long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;
            long maxMemory = runtime.maxMemory() / 1024 / 1024;
            double memoryUsage = (double) usedMemory / maxMemory * 100;
            
            double progress = metrics.getTotalDocuments() > 0 ? 
                (double) metrics.getProcessedDocuments() / metrics.getTotalDocuments() * 100 : 0;
            
            log.info("=== 任务性能报告 ===");
            log.info("任务ID: {}", taskId);
            log.info("源索引: {} -> 目标索引: {}", metrics.getSourceIndex(), metrics.getTargetIndex());
            log.info("进度: {:.2f}% ({}/{} 文档)", progress, metrics.getProcessedDocuments(), metrics.getTotalDocuments());
            log.info("成功: {}, 失败: {}, 成功率: {:.2f}%", 
                metrics.getSuccessDocuments(), metrics.getFailedDocuments(),
                calculateSuccessRate(metrics.getSuccessDocuments(), metrics.getProcessedDocuments()));
            log.info("处理速度: 当前 {:.1f} docs/s, 平均 {:.1f} docs/s", 
                metrics.getCurrentSpeed(), metrics.getAverageSpeed());
            log.info("预计剩余时间: {} 秒", metrics.getEstimatedRemainingTime());
            log.info("内存使用: {}MB / {}MB ({:.1f}%)", usedMemory, maxMemory, memoryUsage);
            log.info("运行时间: {} 秒", (System.currentTimeMillis() - metrics.getStartTime()) / 1000);
            log.info("==================");
        }
    }
    
    /**
     * 记录内存和GC信息
     */
    public void logSystemMetrics() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory() / 1024 / 1024;
        long freeMemory = runtime.freeMemory() / 1024 / 1024;
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory() / 1024 / 1024;
        
        // 获取GC信息
        long totalGcTime = 0;
        long totalGcCount = 0;
        try {
            java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()
                .forEach(gcBean -> {
                    log.debug("GC: {} - 次数: {}, 耗时: {}ms", 
                        gcBean.getName(), gcBean.getCollectionCount(), gcBean.getCollectionTime());
                });
        } catch (Exception e) {
            log.debug("获取GC信息失败: {}", e.getMessage());
        }
        
        log.info("系统指标 - 内存: {}MB 已用/{} MB 最大, 可用处理器: {}", 
            usedMemory, maxMemory, runtime.availableProcessors());
    }
    
    /**
     * 清理完成的任务监控信息
     */
    public void cleanup() {
        taskMetricsMap.entrySet().removeIf(entry -> 
            "COMPLETED".equals(entry.getValue().getStatus()) || 
            "FAILED".equals(entry.getValue().getStatus()));
        
        log.info("清理完成的任务监控信息");
    }
    
    /**
     * 计算成功率
     */
    private double calculateSuccessRate(long successCount, long processedCount) {
        return processedCount > 0 ? (double) successCount / processedCount * 100 : 0;
    }
    
    @Data
    public static class TaskMetrics {
        private String taskId;
        private String sourceIndex;
        private String targetIndex;
        private long totalDocuments;
        private long processedDocuments;
        private long successDocuments;
        private long failedDocuments;
        private double currentSpeed; // docs/second
        private double averageSpeed; // docs/second
        private double overallSpeed; // docs/second
        private long estimatedRemainingTime; // seconds
        private long startTime;
        private long endTime;
        private long lastUpdateTime;
        private String status;
    }
    
    @Data
    public static class GlobalMetrics {
        private int totalTasks;
        private int runningTasks;
        private int completedTasks;
        private int failedTasks;
        private long totalDocumentsProcessed;
        private long totalDocumentsSucceeded;
        private long totalDocumentsFailed;
        private double overallSpeed; // docs/second
        private long duration; // milliseconds
    }
}