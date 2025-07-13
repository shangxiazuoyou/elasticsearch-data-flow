package com.everflowx.esmigration.websocket;

import com.alibaba.fastjson.JSON;
import com.everflowx.esmigration.domain.MigrationTask;
import com.everflowx.esmigration.manager.MigrationTaskManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * 监控WebSocket处理器
 * 负责实时推送任务进度信息
 * 
 * @author everflowx
 */
@Slf4j
@Component
public class MonitorWebSocketHandler implements MigrationTaskManager.TaskProgressListener {
    
    @Autowired
    private SimpMessagingTemplate messagingTemplate;
    
    @Autowired
    private MigrationTaskManager taskManager;
    
    @PostConstruct
    public void init() {
        // 注册为任务进度监听器
        taskManager.addProgressListener(this);
        log.info("监控WebSocket处理器已初始化");
    }
    
    @Override
    public void onTaskCreated(MigrationTask task) {
        Map<String, Object> message = createTaskMessage("TASK_CREATED", task);
        broadcastToAll("/topic/tasks", message);
        log.debug("推送任务创建消息: {}", task.getTaskId());
    }
    
    @Override
    public void onTaskStarted(MigrationTask task) {
        Map<String, Object> message = createTaskMessage("TASK_STARTED", task);
        broadcastToAll("/topic/tasks", message);
        log.debug("推送任务开始消息: {}", task.getTaskId());
    }
    
    @Override
    public void onTaskProgressUpdated(MigrationTask task) {
        Map<String, Object> message = createProgressMessage(task);
        
        // 发送到任务专用频道
        messagingTemplate.convertAndSend("/topic/tasks/" + task.getTaskId() + "/progress", message);
        
        // 发送到全局任务列表频道
        Map<String, Object> summaryMessage = createTaskMessage("TASK_PROGRESS", task);
        broadcastToAll("/topic/tasks", summaryMessage);
        
        log.trace("推送任务进度消息: {} - {}%", task.getTaskId(), task.getProgressPercentage());
    }
    
    @Override
    public void onTaskCompleted(MigrationTask task) {
        Map<String, Object> message = createTaskMessage("TASK_COMPLETED", task);
        broadcastToAll("/topic/tasks", message);
        
        // 发送到任务专用频道
        messagingTemplate.convertAndSend("/topic/tasks/" + task.getTaskId() + "/completed", message);
        
        log.debug("推送任务完成消息: {} - 状态: {}", task.getTaskId(), task.getStatus());
    }
    
    @Override
    public void onTaskCancelled(MigrationTask task) {
        Map<String, Object> message = createTaskMessage("TASK_CANCELLED", task);
        broadcastToAll("/topic/tasks", message);
        log.debug("推送任务取消消息: {}", task.getTaskId());
    }
    
    @Override
    public void onTaskPaused(MigrationTask task) {
        Map<String, Object> message = createTaskMessage("TASK_PAUSED", task);
        broadcastToAll("/topic/tasks", message);
        log.debug("推送任务暂停消息: {}", task.getTaskId());
    }
    
    @Override
    public void onTaskResumed(MigrationTask task) {
        Map<String, Object> message = createTaskMessage("TASK_RESUMED", task);
        broadcastToAll("/topic/tasks", message);
        log.debug("推送任务恢复消息: {}", task.getTaskId());
    }
    
    @Override
    public void onTaskDeleted(MigrationTask task) {
        Map<String, Object> message = createTaskMessage("TASK_DELETED", task);
        broadcastToAll("/topic/tasks", message);
        log.debug("推送任务删除消息: {}", task.getTaskId());
    }
    
    /**
     * 创建任务消息
     */
    private Map<String, Object> createTaskMessage(String eventType, MigrationTask task) {
        Map<String, Object> message = new HashMap<>();
        message.put("eventType", eventType);
        message.put("timestamp", System.currentTimeMillis());
        message.put("task", task.toSummary());
        return message;
    }
    
    /**
     * 创建详细的进度消息
     */
    private Map<String, Object> createProgressMessage(MigrationTask task) {
        Map<String, Object> message = new HashMap<>();
        message.put("eventType", "PROGRESS_UPDATE");
        message.put("timestamp", System.currentTimeMillis());
        message.put("taskId", task.getTaskId());
        
        Map<String, Object> progress = new HashMap<>();
        progress.put("progressPercentage", task.getProgressPercentage());
        progress.put("totalDocuments", task.getTotalDocuments().get());
        progress.put("processedDocuments", task.getProcessedDocuments().get());
        progress.put("successDocuments", task.getSuccessDocuments().get());
        progress.put("failedDocuments", task.getFailedDocuments().get());
        progress.put("currentBatch", task.getCurrentBatch().get());
        progress.put("totalBatches", task.getTotalBatches().get());
        progress.put("currentSpeed", task.getCurrentSpeed().get());
        progress.put("averageSpeed", task.getAverageSpeed().get());
        progress.put("estimatedRemainingTime", task.getEstimatedRemainingTime().get());
        progress.put("currentPhase", task.getCurrentPhase());
        progress.put("durationMs", task.getDurationMs());
        
        message.put("progress", progress);
        return message;
    }
    
    /**
     * 向所有连接的客户端广播消息
     */
    private void broadcastToAll(String destination, Object message) {
        try {
            messagingTemplate.convertAndSend(destination, message);
        } catch (Exception e) {
            log.error("广播消息失败: {}", destination, e);
        }
    }
    
    /**
     * 推送系统统计信息
     */
    public void pushSystemStats() {
        try {
            Map<String, Object> stats = taskManager.getSystemStats();
            Map<String, Object> message = new HashMap<>();
            message.put("eventType", "SYSTEM_STATS");
            message.put("timestamp", System.currentTimeMillis());
            message.put("stats", stats);
            
            broadcastToAll("/topic/system", message);
        } catch (Exception e) {
            log.error("推送系统统计信息失败", e);
        }
    }
    
    /**
     * 推送完整的任务列表
     */
    public void pushTaskList() {
        try {
            Map<String, Object> message = new HashMap<>();
            message.put("eventType", "TASK_LIST");
            message.put("timestamp", System.currentTimeMillis());
            message.put("tasks", taskManager.getTaskSummaries());
            
            broadcastToAll("/topic/tasks", message);
        } catch (Exception e) {
            log.error("推送任务列表失败", e);
        }
    }
    
    /**
     * 发送错误消息
     */
    public void pushErrorMessage(String taskId, String errorMessage) {
        try {
            Map<String, Object> message = new HashMap<>();
            message.put("eventType", "ERROR");
            message.put("timestamp", System.currentTimeMillis());
            message.put("taskId", taskId);
            message.put("errorMessage", errorMessage);
            
            if (taskId != null) {
                messagingTemplate.convertAndSend("/topic/tasks/" + taskId + "/error", message);
            }
            broadcastToAll("/topic/errors", message);
        } catch (Exception e) {
            log.error("推送错误消息失败", e);
        }
    }
    
    /**
     * 发送警告消息
     */
    public void pushWarningMessage(String taskId, String warningMessage) {
        try {
            Map<String, Object> message = new HashMap<>();
            message.put("eventType", "WARNING");
            message.put("timestamp", System.currentTimeMillis());
            message.put("taskId", taskId);
            message.put("warningMessage", warningMessage);
            
            if (taskId != null) {
                messagingTemplate.convertAndSend("/topic/tasks/" + taskId + "/warning", message);
            }
            broadcastToAll("/topic/warnings", message);
        } catch (Exception e) {
            log.error("推送警告消息失败", e);
        }
    }
    
    /**
     * 推送系统统计信息（增强版本）
     */
    public void pushSystemStats(Map<String, Object> statsData) {
        try {
            Map<String, Object> message = new HashMap<>();
            message.put("eventType", "ENHANCED_SYSTEM_STATS");
            message.putAll(statsData);
            
            broadcastToAll("/topic/system", message);
        } catch (Exception e) {
            log.error("推送增强系统统计信息失败", e);
        }
    }
    
    /**
     * 向指定频道推送消息
     */
    public void pushMessage(String destination, Object messageData) {
        try {
            broadcastToAll(destination, messageData);
            log.debug("推送消息到频道: {}", destination);
        } catch (Exception e) {
            log.error("推送消息到频道 {} 失败", destination, e);
        }
    }
    
    /**
     * 推送性能告警
     */
    public void pushPerformanceAlert(String alertType, String message, Map<String, Object> metrics) {
        try {
            Map<String, Object> alertMessage = new HashMap<>();
            alertMessage.put("eventType", "PERFORMANCE_ALERT");
            alertMessage.put("alertType", alertType);
            alertMessage.put("message", message);
            alertMessage.put("metrics", metrics);
            alertMessage.put("timestamp", System.currentTimeMillis());
            alertMessage.put("severity", getAlertSeverity(alertType));
            
            broadcastToAll("/topic/alerts", alertMessage);
            log.warn("推送性能告警: {} - {}", alertType, message);
        } catch (Exception e) {
            log.error("推送性能告警失败", e);
        }
    }
    
    /**
     * 推送系统健康状态
     */
    public void pushHealthStatus(Map<String, Object> healthData) {
        try {
            Map<String, Object> message = new HashMap<>();
            message.put("eventType", "HEALTH_STATUS");
            message.put("timestamp", System.currentTimeMillis());
            message.putAll(healthData);
            
            broadcastToAll("/topic/health", message);
        } catch (Exception e) {
            log.error("推送系统健康状态失败", e);
        }
    }
    
    /**
     * 批量推送多个消息
     */
    public void pushBatchMessages(Map<String, Object> messages) {
        try {
            Map<String, Object> batchMessage = new HashMap<>();
            batchMessage.put("eventType", "BATCH_UPDATE");
            batchMessage.put("timestamp", System.currentTimeMillis());
            batchMessage.put("updates", messages);
            
            broadcastToAll("/topic/batch", batchMessage);
        } catch (Exception e) {
            log.error("批量推送消息失败", e);
        }
    }
    
    /**
     * 获取告警严重程度
     */
    private String getAlertSeverity(String alertType) {
        switch (alertType.toUpperCase()) {
            case "HIGH_ERROR_RATE":
            case "MEMORY_CRITICAL":
            case "SYSTEM_OVERLOAD":
                return "CRITICAL";
            case "SLOW_PERFORMANCE":
            case "HIGH_GC_TIME":
            case "THREAD_POOL_FULL":
                return "WARNING";
            case "DISK_SPACE_LOW":
            case "CONNECTION_TIMEOUT":
                return "INFO";
            default:
                return "INFO";
        }
    }
}