package com.everflowx.esmigration.service;

import com.everflowx.esmigration.domain.MigrationTask;
import com.everflowx.esmigration.domain.TaskStatus;
import com.everflowx.esmigration.exception.GlobalExceptionHandler;
import com.everflowx.esmigration.manager.MigrationTaskManager;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.OptionalDouble;

/**
 * 系统统计服务
 * 收集和计算系统运行统计数据
 * 
 * @author everflowx
 */
@Slf4j
@Service
public class SystemStatisticsService {
    
    @Autowired
    private MigrationTaskManager taskManager;
    
    @Autowired
    private GlobalExceptionHandler globalExceptionHandler;
    
    // 历史统计数据缓存（最近24小时，每分钟一个数据点）
    private final Map<LocalDateTime, StatisticsSnapshot> historicalStats = new ConcurrentHashMap<>();
    
    // 性能指标统计
    private final AtomicLong totalProcessedDocuments = new AtomicLong(0);
    private final AtomicLong totalProcessingTime = new AtomicLong(0);
    private final Map<String, Double> performanceMetrics = new ConcurrentHashMap<>();
    
    /**
     * 获取完整的系统统计信息
     */
    public Map<String, Object> getComprehensiveStats() {
        Map<String, Object> stats = new HashMap<>();
        
        // 基础任务统计
        stats.put("taskStats", getTaskStatistics());
        
        // 系统资源统计
        stats.put("systemResources", getSystemResourceStats());
        
        // 性能统计
        stats.put("performanceStats", getPerformanceStatistics());
        
        // 异常统计
        stats.put("exceptionStats", globalExceptionHandler.getExceptionStats());
        
        // 历史趋势数据
        stats.put("trendData", getHistoricalTrends());
        
        // 实时指标
        stats.put("realtimeMetrics", getRealtimeMetrics());
        
        stats.put("timestamp", System.currentTimeMillis());
        stats.put("generatedAt", LocalDateTime.now().toString());
        
        return stats;
    }
    
    /**
     * 获取任务统计信息
     */
    public Map<String, Object> getTaskStatistics() {
        Map<String, Object> taskStats = new HashMap<>();
        
        Collection<MigrationTask> allTasks = taskManager.getAllTasks();
        
        // 按状态分组统计
        Map<TaskStatus, Long> statusCounts = allTasks.stream()
            .collect(Collectors.groupingBy(
                MigrationTask::getStatus,
                Collectors.counting()
            ));
        
        taskStats.put("totalTasks", allTasks.size());
        taskStats.put("runningTasks", statusCounts.getOrDefault(TaskStatus.RUNNING, 0L));
        taskStats.put("completedTasks", statusCounts.getOrDefault(TaskStatus.COMPLETED, 0L));
        taskStats.put("failedTasks", statusCounts.getOrDefault(TaskStatus.FAILED, 0L));
        taskStats.put("pendingTasks", statusCounts.getOrDefault(TaskStatus.PENDING, 0L));
        taskStats.put("pausedTasks", statusCounts.getOrDefault(TaskStatus.PAUSED, 0L));
        taskStats.put("cancelledTasks", statusCounts.getOrDefault(TaskStatus.CANCELLED, 0L));
        
        // 文档处理统计
        long totalDocs = 0;
        long processedDocs = 0;
        long successDocs = 0;
        long failedDocs = 0;
        
        for (MigrationTask task : allTasks) {
            totalDocs += task.getTotalDocuments().get();
            processedDocs += task.getProcessedDocuments().get();
            successDocs += task.getSuccessDocuments().get();
            failedDocs += task.getFailedDocuments().get();
        }
        
        taskStats.put("totalDocuments", totalDocs);
        taskStats.put("processedDocuments", processedDocs);
        taskStats.put("successDocuments", successDocs);
        taskStats.put("failedDocuments", failedDocs);
        
        // 计算成功率和完成率
        if (processedDocs > 0) {
            taskStats.put("successRate", String.format("%.2f%%", (double) successDocs / processedDocs * 100));
        } else {
            taskStats.put("successRate", "0.00%");
        }
        
        if (totalDocs > 0) {
            taskStats.put("completionRate", String.format("%.2f%%", (double) processedDocs / totalDocs * 100));
        } else {
            taskStats.put("completionRate", "0.00%");
        }
        
        // 任务持续时间统计
        long totalDuration = 0;
        int completedCount = 0;
        for (MigrationTask task : allTasks) {
            if (task.getStatus() == TaskStatus.COMPLETED) {
                totalDuration += task.getDurationMs();
                completedCount++;
            }
        }
        
        taskStats.put("averageTaskDuration", completedCount > 0 ? 
            String.format("%.2f分钟", (double) totalDuration / completedCount / 60000.0) : "暂无数据");
        
        return taskStats;
    }
    
    /**
     * 获取系统资源统计
     */
    public Map<String, Object> getSystemResourceStats() {
        Map<String, Object> resourceStats = new HashMap<>();
        
        // JVM内存统计
        MemoryMXBean memoryMBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemory = memoryMBean.getHeapMemoryUsage();
        MemoryUsage nonHeapMemory = memoryMBean.getNonHeapMemoryUsage();
        
        Map<String, Object> memoryStats = new HashMap<>();
        memoryStats.put("heapUsed", heapMemory.getUsed() / 1024 / 1024); // MB
        memoryStats.put("heapMax", heapMemory.getMax() / 1024 / 1024); // MB
        memoryStats.put("heapCommitted", heapMemory.getCommitted() / 1024 / 1024); // MB
        memoryStats.put("heapUsagePercent", String.format("%.2f%%", 
            (double) heapMemory.getUsed() / heapMemory.getMax() * 100));
        
        memoryStats.put("nonHeapUsed", nonHeapMemory.getUsed() / 1024 / 1024); // MB
        memoryStats.put("nonHeapMax", nonHeapMemory.getMax() == -1 ? "无限制" : 
            nonHeapMemory.getMax() / 1024 / 1024 + "MB");
        
        resourceStats.put("memory", memoryStats);
        
        // GC统计
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        Map<String, Object> gcStats = new HashMap<>();
        long totalGcTime = 0;
        long totalGcCount = 0;
        
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            totalGcTime += gcBean.getCollectionTime();
            totalGcCount += gcBean.getCollectionCount();
            
            Map<String, Object> gcDetail = new HashMap<>();
            gcDetail.put("collectionCount", gcBean.getCollectionCount());
            gcDetail.put("collectionTime", gcBean.getCollectionTime() + "ms");
            gcStats.put(gcBean.getName(), gcDetail);
        }
        
        gcStats.put("totalGcTime", totalGcTime + "ms");
        gcStats.put("totalGcCount", totalGcCount);
        resourceStats.put("garbageCollection", gcStats);
        
        // 线程统计
        Map<String, Object> threadStats = new HashMap<>();
        threadStats.put("currentThreadCount", ManagementFactory.getThreadMXBean().getThreadCount());
        threadStats.put("peakThreadCount", ManagementFactory.getThreadMXBean().getPeakThreadCount());
        threadStats.put("totalStartedThreadCount", ManagementFactory.getThreadMXBean().getTotalStartedThreadCount());
        resourceStats.put("threads", threadStats);
        
        // 运行时统计
        Runtime runtime = Runtime.getRuntime();
        Map<String, Object> runtimeStats = new HashMap<>();
        runtimeStats.put("availableProcessors", runtime.availableProcessors());
        runtimeStats.put("totalMemoryMB", runtime.totalMemory() / 1024 / 1024);
        runtimeStats.put("freeMemoryMB", runtime.freeMemory() / 1024 / 1024);
        runtimeStats.put("maxMemoryMB", runtime.maxMemory() / 1024 / 1024);
        
        resourceStats.put("runtime", runtimeStats);
        
        return resourceStats;
    }
    
    /**
     * 获取性能统计
     */
    public Map<String, Object> getPerformanceStatistics() {
        Map<String, Object> performanceStats = new HashMap<>();
        
        Collection<MigrationTask> runningTasks = taskManager.getRunningTasks();
        
        if (!runningTasks.isEmpty()) {
            // 当前处理速度统计
            long totalSpeedLong = 0;
            for (MigrationTask task : runningTasks) {
                totalSpeedLong += task.getCurrentSpeed().get();
            }
            double totalSpeed = (double) totalSpeedLong;
            double avgSpeed = totalSpeed / runningTasks.size();
            
            performanceStats.put("currentTotalSpeed", String.format("%.0f 文档/秒", totalSpeed));
            performanceStats.put("currentAverageSpeed", String.format("%.0f 文档/秒", avgSpeed));
            
            // 估计剩余时间
            long totalRemainingLong = 0;
            for (MigrationTask task : runningTasks) {
                totalRemainingLong += task.getEstimatedRemainingTime().get();
            }
            double totalRemainingTime = runningTasks.size() > 0 ? (double) totalRemainingLong / runningTasks.size() : 0.0;
            
            performanceStats.put("estimatedCompletionTime", 
                totalRemainingTime > 0 ? String.format("%.1f 分钟", totalRemainingTime / 60.0) : "计算中...");
        } else {
            performanceStats.put("currentTotalSpeed", "0 文档/秒");
            performanceStats.put("currentAverageSpeed", "0 文档/秒");
            performanceStats.put("estimatedCompletionTime", "无运行中任务");
        }
        
        // 历史性能指标
        performanceStats.put("totalProcessedDocuments", totalProcessedDocuments.get());
        performanceStats.put("totalProcessingTimeHours", 
            String.format("%.2f", totalProcessingTime.get() / 3600000.0));
        
        if (totalProcessingTime.get() > 0) {
            double overallThroughput = (double) totalProcessedDocuments.get() / 
                (totalProcessingTime.get() / 1000.0);
            performanceStats.put("overallThroughput", String.format("%.2f 文档/秒", overallThroughput));
        } else {
            performanceStats.put("overallThroughput", "暂无数据");
        }
        
        return performanceStats;
    }
    
    /**
     * 获取历史趋势数据（最近24小时）
     */
    public Map<String, Object> getHistoricalTrends() {
        Map<String, Object> trends = new HashMap<>();
        
        // 清理24小时前的数据
        LocalDateTime cutoff = LocalDateTime.now().minusHours(24);
        historicalStats.entrySet().removeIf(entry -> entry.getKey().isBefore(cutoff));
        
        // 按小时聚合数据
        Map<Integer, List<StatisticsSnapshot>> hourlyData = historicalStats.entrySet().stream()
            .collect(Collectors.groupingBy(
                entry -> entry.getKey().getHour(),
                Collectors.mapping(Map.Entry::getValue, Collectors.toList())
            ));
        
        List<Map<String, Object>> hourlyTrends = new ArrayList<>();
        for (int hour = 0; hour < 24; hour++) {
            List<StatisticsSnapshot> snapshots = hourlyData.getOrDefault(hour, new ArrayList<>());
            
            Map<String, Object> hourData = new HashMap<>();
            hourData.put("hour", hour);
            hourData.put("taskCount", snapshots.stream().mapToInt(s -> s.taskCount).average().orElse(0));
            hourData.put("throughput", snapshots.stream().mapToDouble(s -> s.throughput).average().orElse(0));
            hourData.put("errorRate", snapshots.stream().mapToDouble(s -> s.errorRate).average().orElse(0));
            
            hourlyTrends.add(hourData);
        }
        
        trends.put("hourlyTrends", hourlyTrends);
        trends.put("dataPoints", historicalStats.size());
        
        return trends;
    }
    
    /**
     * 获取实时指标
     */
    public Map<String, Object> getRealtimeMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        // 系统负载指标
        double systemLoad = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
        metrics.put("systemLoadAverage", systemLoad >= 0 ? String.format("%.2f", systemLoad) : "不支持");
        
        // 当前活跃任务的详细指标
        Collection<MigrationTask> runningTasks = taskManager.getRunningTasks();
        if (!runningTasks.isEmpty()) {
            Map<String, Object> activeTaskMetrics = new HashMap<>();
            
            for (MigrationTask task : runningTasks) {
                Map<String, Object> taskMetric = new HashMap<>();
                taskMetric.put("progress", task.getProgressPercentage());
                taskMetric.put("speed", task.getCurrentSpeed().get());
                taskMetric.put("phase", task.getCurrentPhase());
                taskMetric.put("duration", task.getDurationMs() / 1000); // 秒
                
                activeTaskMetrics.put(task.getTaskId(), taskMetric);
            }
            
            metrics.put("activeTasks", activeTaskMetrics);
        }
        
        // 当前时间戳
        metrics.put("serverTime", LocalDateTime.now().toString());
        metrics.put("uptime", ManagementFactory.getRuntimeMXBean().getUptime() / 1000); // 秒
        
        return metrics;
    }
    
    /**
     * 记录统计快照
     */
    public void recordStatisticsSnapshot() {
        try {
            StatisticsSnapshot snapshot = new StatisticsSnapshot();
            snapshot.timestamp = LocalDateTime.now();
            snapshot.taskCount = taskManager.getAllTasks().size();
            
            // 计算当前吞吐量
            Collection<MigrationTask> runningTasks = taskManager.getRunningTasks();
            long throughputLong = 0;
            for (MigrationTask task : runningTasks) {
                throughputLong += task.getCurrentSpeed().get();
            }
            snapshot.throughput = (double) throughputLong;
            
            // 计算错误率
            Map<String, Object> exceptionStats = globalExceptionHandler.getExceptionStats();
            long totalExceptions = (Long) exceptionStats.getOrDefault("totalExceptions", 0L);
            snapshot.errorRate = totalExceptions > 0 ? totalExceptions / 1000.0 : 0.0; // 归一化错误率
            
            historicalStats.put(snapshot.timestamp, snapshot);
            
        } catch (Exception e) {
            log.error("记录统计快照失败", e);
        }
    }
    
    /**
     * 更新性能指标
     */
    public void updatePerformanceMetrics(long processedDocs, long processingTimeMs) {
        totalProcessedDocuments.addAndGet(processedDocs);
        totalProcessingTime.addAndGet(processingTimeMs);
        
        // 更新其他性能指标
        performanceMetrics.put("lastUpdateTime", (double) System.currentTimeMillis());
    }
    
    /**
     * 统计快照数据结构
     */
    @Data
    private static class StatisticsSnapshot {
        LocalDateTime timestamp;
        int taskCount;
        double throughput;
        double errorRate;
    }
}