package com.everflowx.esmigration.task;

import com.everflowx.esmigration.service.SystemStatisticsService;
import com.everflowx.esmigration.websocket.MonitorWebSocketHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * 系统统计定时任务
 * 定期收集和推送系统统计数据
 * 
 * @author everflowx
 */
@Slf4j
@Component
public class SystemStatisticsTask {
    
    @Autowired
    private SystemStatisticsService statisticsService;
    
    @Autowired
    private MonitorWebSocketHandler webSocketHandler;
    
    /**
     * 每分钟记录统计快照
     */
    @Scheduled(fixedRate = 60000) // 60秒
    public void recordStatisticsSnapshot() {
        try {
            statisticsService.recordStatisticsSnapshot();
            log.debug("系统统计快照记录完成");
        } catch (Exception e) {
            log.error("记录系统统计快照失败", e);
        }
    }
    
    /**
     * 每5秒推送基础统计数据
     */
    @Scheduled(fixedRate = 5000) // 5秒
    public void pushBasicStats() {
        try {
            Map<String, Object> taskStats = statisticsService.getTaskStatistics();
            Map<String, Object> performanceStats = statisticsService.getPerformanceStatistics();
            
            // 构建推送数据
            Map<String, Object> pushData = new HashMap<>();
            pushData.put("taskStats", taskStats);
            pushData.put("performanceStats", performanceStats);
            pushData.put("timestamp", System.currentTimeMillis());
            pushData.put("eventType", "ENHANCED_SYSTEM_STATS");
            
            webSocketHandler.pushSystemStats(pushData);
            
            log.trace("基础统计数据推送完成");
            
        } catch (Exception e) {
            log.error("推送基础统计数据失败", e);
        }
    }
    
    /**
     * 每30秒推送系统资源统计
     */
    @Scheduled(fixedRate = 30000) // 30秒
    public void pushSystemResourceStats() {
        try {
            Map<String, Object> resourceStats = statisticsService.getSystemResourceStats();
            Map<String, Object> realtimeMetrics = statisticsService.getRealtimeMetrics();
            
            // 构建推送数据
            Map<String, Object> pushData = new HashMap<>();
            pushData.put("type", "SYSTEM_RESOURCES");
            pushData.put("resourceStats", resourceStats);
            pushData.put("realtimeMetrics", realtimeMetrics);
            pushData.put("timestamp", System.currentTimeMillis());
            pushData.put("eventType", "SYSTEM_RESOURCE_UPDATE");
            
            webSocketHandler.pushMessage("/topic/system-resources", pushData);
            
            log.debug("系统资源统计推送完成");
            
        } catch (Exception e) {
            log.error("推送系统资源统计失败", e);
        }
    }
    
    /**
     * 每10分钟推送完整统计报告
     */
    @Scheduled(fixedRate = 600000) // 10分钟
    public void pushComprehensiveReport() {
        try {
            Map<String, Object> comprehensiveStats = statisticsService.getComprehensiveStats();
            
            // 构建推送数据
            Map<String, Object> pushData = new HashMap<>();
            pushData.put("type", "COMPREHENSIVE_REPORT");
            pushData.put("stats", comprehensiveStats);
            pushData.put("timestamp", System.currentTimeMillis());
            pushData.put("eventType", "COMPREHENSIVE_STATS_UPDATE");
            
            webSocketHandler.pushMessage("/topic/comprehensive-stats", pushData);
            
            // 安全获取数据点数量
            Object trendData = comprehensiveStats.get("trendData");
            int dataPoints = 0;
            if (trendData instanceof Map) {
                Object dataPointsObj = ((Map<String, Object>) trendData).get("dataPoints");
                if (dataPointsObj instanceof Number) {
                    dataPoints = ((Number) dataPointsObj).intValue();
                }
            }
            
            log.info("推送完整统计报告完成，数据点数量: {}", dataPoints);
            
        } catch (Exception e) {
            log.error("推送完整统计报告失败", e);
        }
    }
    
    /**
     * 每小时清理历史数据
     */
    @Scheduled(fixedRate = 3600000) // 1小时
    public void cleanupHistoricalData() {
        try {
            // 触发统计服务的历史数据清理
            statisticsService.getHistoricalTrends(); // 内部会清理过期数据
            
            log.info("历史数据清理完成");
            
        } catch (Exception e) {
            log.error("清理历史数据失败", e);
        }
    }
}