package com.everflowx.esmigration.manager;

import com.everflowx.esmigration.domain.MigrationTask;
import com.everflowx.esmigration.domain.TaskStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * 迁移任务管理器
 * 负责管理所有迁移任务的生命周期
 * 
 * @author everflowx
 */
@Slf4j
@Component
public class MigrationTaskManager {
    
    /**
     * 任务存储 - 使用ConcurrentHashMap保证线程安全
     */
    private final ConcurrentHashMap<String, MigrationTask> tasks = new ConcurrentHashMap<>();
    
    /**
     * 任务进度监听器列表
     */
    private final List<TaskProgressListener> progressListeners = new CopyOnWriteArrayList<>();
    
    /**
     * 最大保存的已完成任务数量
     */
    private static final int MAX_COMPLETED_TASKS = 100;
    
    /**
     * 创建新任务
     */
    public MigrationTask createTask(String taskName, String taskType) {
        MigrationTask task = new MigrationTask();
        task.setTaskId(generateTaskId());
        task.setTaskName(taskName);
        task.setTaskType(taskType);
        task.setStatus(TaskStatus.PENDING);
        task.setCreateTime(new Date());
        task.setCurrentPhase("任务已创建，等待执行");
        
        tasks.put(task.getTaskId(), task);
        
        log.info("创建新任务: {} [{}] - {}", task.getTaskId(), taskType, taskName);
        notifyTaskCreated(task);
        
        return task;
    }
    
    /**
     * 开始执行任务
     */
    public void startTask(String taskId) {
        MigrationTask task = tasks.get(taskId);
        if (task != null && task.getStatus() == TaskStatus.PENDING) {
            task.setStatus(TaskStatus.RUNNING);
            task.setStartTime(new Date());
            task.setCurrentPhase("任务执行中");
            task.setLastUpdateTime(new Date());
            
            log.info("开始执行任务: {} [{}]", taskId, task.getTaskName());
            notifyTaskStarted(task);
        }
    }
    
    /**
     * 更新任务进度
     */
    public void updateTaskProgress(String taskId, long totalDocuments, long processedDocuments, 
                                 long successDocuments, long failedDocuments, String currentPhase) {
        MigrationTask task = tasks.get(taskId);
        if (task != null && task.isRunning()) {
            task.getTotalDocuments().set(totalDocuments);
            task.getProcessedDocuments().set(processedDocuments);
            task.getSuccessDocuments().set(successDocuments);
            task.getFailedDocuments().set(failedDocuments);
            task.setCurrentPhase(currentPhase);
            task.updateProgress();
            
            notifyTaskProgressUpdated(task);
        }
    }
    
    /**
     * 更新批次进度
     */
    public void updateBatchProgress(String taskId, long currentBatch, long totalBatches) {
        MigrationTask task = tasks.get(taskId);
        if (task != null && task.isRunning()) {
            task.getCurrentBatch().set(currentBatch);
            task.getTotalBatches().set(totalBatches);
            task.setLastUpdateTime(new Date());
            
            notifyTaskProgressUpdated(task);
        }
    }
    
    /**
     * 设置任务当前处理速度
     */
    public void updateTaskSpeed(String taskId, long currentSpeed) {
        MigrationTask task = tasks.get(taskId);
        if (task != null && task.isRunning()) {
            task.getCurrentSpeed().set(currentSpeed);
            task.updateProgress();
            
            notifyTaskProgressUpdated(task);
        }
    }
    
    /**
     * 完成任务
     */
    public void completeTask(String taskId, boolean success, String errorMessage) {
        MigrationTask task = tasks.get(taskId);
        if (task != null && task.isRunning()) {
            task.setStatus(success ? TaskStatus.COMPLETED : TaskStatus.FAILED);
            task.setEndTime(new Date());
            task.setErrorMessage(errorMessage);
            task.setCurrentPhase(success ? "任务执行完成" : "任务执行失败");
            task.updateProgress();
            
            log.info("任务执行完成: {} [{}] - 状态: {}", taskId, task.getTaskName(), task.getStatus());
            notifyTaskCompleted(task);
            
            // 清理过多的已完成任务
            cleanupCompletedTasks();
        }
    }
    
    /**
     * 取消任务
     */
    public boolean cancelTask(String taskId) {
        MigrationTask task = tasks.get(taskId);
        if (task != null && !task.isFinished()) {
            task.setStatus(TaskStatus.CANCELLED);
            task.setEndTime(new Date());
            task.setCurrentPhase("任务已取消");
            task.setLastUpdateTime(new Date());
            
            log.info("取消任务: {} [{}]", taskId, task.getTaskName());
            notifyTaskCancelled(task);
            return true;
        }
        return false;
    }
    
    /**
     * 暂停任务
     */
    public boolean pauseTask(String taskId) {
        MigrationTask task = tasks.get(taskId);
        if (task != null && task.getStatus() == TaskStatus.RUNNING) {
            task.setStatus(TaskStatus.PAUSED);
            task.setCurrentPhase("任务已暂停");
            task.setLastUpdateTime(new Date());
            
            log.info("暂停任务: {} [{}]", taskId, task.getTaskName());
            notifyTaskPaused(task);
            return true;
        }
        return false;
    }
    
    /**
     * 恢复任务
     */
    public boolean resumeTask(String taskId) {
        MigrationTask task = tasks.get(taskId);
        if (task != null && task.getStatus() == TaskStatus.PAUSED) {
            task.setStatus(TaskStatus.RUNNING);
            task.setCurrentPhase("任务已恢复");
            task.setLastUpdateTime(new Date());
            
            log.info("恢复任务: {} [{}]", taskId, task.getTaskName());
            notifyTaskResumed(task);
            return true;
        }
        return false;
    }
    
    /**
     * 获取任务详情
     */
    public MigrationTask getTask(String taskId) {
        return tasks.get(taskId);
    }
    
    /**
     * 获取所有任务列表
     */
    public List<MigrationTask> getAllTasks() {
        return new ArrayList<>(tasks.values());
    }
    
    /**
     * 获取任务摘要列表
     */
    public List<MigrationTask.MigrationTaskSummary> getTaskSummaries() {
        return tasks.values().stream()
                .sorted((t1, t2) -> t2.getCreateTime().compareTo(t1.getCreateTime()))
                .map(MigrationTask::toSummary)
                .collect(Collectors.toList());
    }
    
    /**
     * 根据状态获取任务
     */
    public List<MigrationTask> getTasksByStatus(TaskStatus status) {
        return tasks.values().stream()
                .filter(task -> task.getStatus() == status)
                .sorted((t1, t2) -> t2.getCreateTime().compareTo(t1.getCreateTime()))
                .collect(Collectors.toList());
    }
    
    /**
     * 获取正在运行的任务
     */
    public List<MigrationTask> getRunningTasks() {
        return getTasksByStatus(TaskStatus.RUNNING);
    }
    
    /**
     * 获取系统统计信息
     */
    public Map<String, Object> getSystemStats() {
        Map<String, Object> stats = new HashMap<>();
        
        Map<TaskStatus, Long> statusCounts = tasks.values().stream()
                .collect(Collectors.groupingBy(
                    MigrationTask::getStatus,
                    Collectors.counting()
                ));
        
        stats.put("totalTasks", tasks.size());
        stats.put("runningTasks", statusCounts.getOrDefault(TaskStatus.RUNNING, 0L));
        stats.put("completedTasks", statusCounts.getOrDefault(TaskStatus.COMPLETED, 0L));
        stats.put("failedTasks", statusCounts.getOrDefault(TaskStatus.FAILED, 0L));
        stats.put("pendingTasks", statusCounts.getOrDefault(TaskStatus.PENDING, 0L));
        stats.put("cancelledTasks", statusCounts.getOrDefault(TaskStatus.CANCELLED, 0L));
        stats.put("pausedTasks", statusCounts.getOrDefault(TaskStatus.PAUSED, 0L));
        
        // 计算总处理文档数
        long totalProcessedDocs = tasks.values().stream()
                .mapToLong(task -> task.getProcessedDocuments().get())
                .sum();
        stats.put("totalProcessedDocuments", totalProcessedDocs);
        
        return stats;
    }
    
    /**
     * 删除任务
     */
    public boolean deleteTask(String taskId) {
        MigrationTask task = tasks.get(taskId);
        if (task != null && task.isFinished()) {
            tasks.remove(taskId);
            log.info("删除任务: {} [{}]", taskId, task.getTaskName());
            notifyTaskDeleted(task);
            return true;
        }
        return false;
    }
    
    /**
     * 清理已完成的任务（保留最近的N个）
     */
    private void cleanupCompletedTasks() {
        List<MigrationTask> completedTasks = tasks.values().stream()
                .filter(MigrationTask::isFinished)
                .sorted((t1, t2) -> t2.getEndTime().compareTo(t1.getEndTime()))
                .collect(Collectors.toList());
        
        if (completedTasks.size() > MAX_COMPLETED_TASKS) {
            List<MigrationTask> tasksToRemove = completedTasks.subList(MAX_COMPLETED_TASKS, completedTasks.size());
            for (MigrationTask task : tasksToRemove) {
                tasks.remove(task.getTaskId());
                log.debug("自动清理已完成任务: {} [{}]", task.getTaskId(), task.getTaskName());
            }
        }
    }
    
    /**
     * 生成任务ID
     */
    private String generateTaskId() {
        return "TASK_" + System.currentTimeMillis() + "_" + new Random().nextInt(1000);
    }
    
    // ========== 监听器相关方法 ==========
    
    /**
     * 添加进度监听器
     */
    public void addProgressListener(TaskProgressListener listener) {
        progressListeners.add(listener);
    }
    
    /**
     * 移除进度监听器
     */
    public void removeProgressListener(TaskProgressListener listener) {
        progressListeners.remove(listener);
    }
    
    private void notifyTaskCreated(MigrationTask task) {
        for (TaskProgressListener listener : progressListeners) {
            try {
                listener.onTaskCreated(task);
            } catch (Exception e) {
                log.error("通知任务创建失败", e);
            }
        }
    }
    
    private void notifyTaskStarted(MigrationTask task) {
        for (TaskProgressListener listener : progressListeners) {
            try {
                listener.onTaskStarted(task);
            } catch (Exception e) {
                log.error("通知任务开始失败", e);
            }
        }
    }
    
    private void notifyTaskProgressUpdated(MigrationTask task) {
        for (TaskProgressListener listener : progressListeners) {
            try {
                listener.onTaskProgressUpdated(task);
            } catch (Exception e) {
                log.error("通知任务进度更新失败", e);
            }
        }
    }
    
    private void notifyTaskCompleted(MigrationTask task) {
        for (TaskProgressListener listener : progressListeners) {
            try {
                listener.onTaskCompleted(task);
            } catch (Exception e) {
                log.error("通知任务完成失败", e);
            }
        }
    }
    
    private void notifyTaskCancelled(MigrationTask task) {
        for (TaskProgressListener listener : progressListeners) {
            try {
                listener.onTaskCancelled(task);
            } catch (Exception e) {
                log.error("通知任务取消失败", e);
            }
        }
    }
    
    private void notifyTaskPaused(MigrationTask task) {
        for (TaskProgressListener listener : progressListeners) {
            try {
                listener.onTaskPaused(task);
            } catch (Exception e) {
                log.error("通知任务暂停失败", e);
            }
        }
    }
    
    private void notifyTaskResumed(MigrationTask task) {
        for (TaskProgressListener listener : progressListeners) {
            try {
                listener.onTaskResumed(task);
            } catch (Exception e) {
                log.error("通知任务恢复失败", e);
            }
        }
    }
    
    private void notifyTaskDeleted(MigrationTask task) {
        for (TaskProgressListener listener : progressListeners) {
            try {
                listener.onTaskDeleted(task);
            } catch (Exception e) {
                log.error("通知任务删除失败", e);
            }
        }
    }
    
    /**
     * 任务进度监听器接口
     */
    public interface TaskProgressListener {
        default void onTaskCreated(MigrationTask task) {}
        default void onTaskStarted(MigrationTask task) {}
        default void onTaskProgressUpdated(MigrationTask task) {}
        default void onTaskCompleted(MigrationTask task) {}
        default void onTaskCancelled(MigrationTask task) {}
        default void onTaskPaused(MigrationTask task) {}
        default void onTaskResumed(MigrationTask task) {}
        default void onTaskDeleted(MigrationTask task) {}
    }
}