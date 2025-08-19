package com.everflowx.esmigration.checkpoint;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 高级断点管理器
 * 支持字段级别的细粒度断点续传和恢复
 * 
 * @author everflowx
 */
@Slf4j
@Component
public class AdvancedCheckpointManager {
    
    // 内存中的断点缓存，key为taskId
    private final Map<String, List<FieldLevelCheckpoint>> checkpointCache = new ConcurrentHashMap<>();
    
    // 断点序列号生成器
    private final AtomicLong checkpointSequence = new AtomicLong(1);
    
    // 断点过期时间：24小时
    private static final long CHECKPOINT_EXPIRATION_MS = 24 * 60 * 60 * 1000L;
    
    // 最大断点保留数量
    private static final int MAX_CHECKPOINTS_PER_TASK = 1000;
    
    /**
     * 创建文档级别断点
     */
    public FieldLevelCheckpoint createDocumentCheckpoint(String taskId, String docId, 
                                                        String scrollId, Object[] searchAfter) {
        FieldLevelCheckpoint checkpoint = new FieldLevelCheckpoint();
        checkpoint.setCheckpointId(generateCheckpointId());
        checkpoint.setTaskId(taskId);
        checkpoint.setType(FieldLevelCheckpoint.CheckpointType.DOCUMENT_LEVEL);
        checkpoint.setCreateTime(new Date());
        checkpoint.setLastUpdateTime(new Date());
        checkpoint.setStatus(FieldLevelCheckpoint.CheckpointStatus.PENDING);
        
        FieldLevelCheckpoint.DocumentCheckpoint docCheckpoint = new FieldLevelCheckpoint.DocumentCheckpoint();
        docCheckpoint.setLastProcessedDocId(docId);
        docCheckpoint.setLastScrollId(scrollId);
        docCheckpoint.setLastSearchAfter(searchAfter);
        docCheckpoint.setLastProcessedTimestamp(System.currentTimeMillis());
        docCheckpoint.setProcessedDocCount(0);
        docCheckpoint.setFailedDocIds(new HashSet<>());
        docCheckpoint.setPartiallyProcessedDocIds(new HashSet<>());
        
        checkpoint.setDocumentCheckpoint(docCheckpoint);
        
        saveCheckpoint(checkpoint);
        log.debug("创建文档级别断点: taskId={}, docId={}", taskId, docId);
        
        return checkpoint;
    }
    
    /**
     * 创建字段级别断点
     */
    public FieldLevelCheckpoint createFieldCheckpoint(String taskId, String docId, 
                                                     String fieldName, Object originalValue) {
        FieldLevelCheckpoint checkpoint = new FieldLevelCheckpoint();
        checkpoint.setCheckpointId(generateCheckpointId());
        checkpoint.setTaskId(taskId);
        checkpoint.setType(FieldLevelCheckpoint.CheckpointType.FIELD_LEVEL);
        checkpoint.setCreateTime(new Date());
        checkpoint.setLastUpdateTime(new Date());
        checkpoint.setStatus(FieldLevelCheckpoint.CheckpointStatus.PENDING);
        
        Map<String, FieldLevelCheckpoint.FieldCheckpoint> fieldCheckpoints = new HashMap<>();
        FieldLevelCheckpoint.FieldCheckpoint fieldCheckpoint = new FieldLevelCheckpoint.FieldCheckpoint();
        fieldCheckpoint.setFieldName(fieldName);
        fieldCheckpoint.setDocumentId(docId);
        fieldCheckpoint.setOriginalValue(originalValue);
        fieldCheckpoint.setStatus(FieldLevelCheckpoint.FieldCheckpoint.FieldProcessStatus.PENDING);
        fieldCheckpoint.setRetryCount(0);
        
        fieldCheckpoints.put(fieldName, fieldCheckpoint);
        checkpoint.setFieldCheckpoints(fieldCheckpoints);
        
        saveCheckpoint(checkpoint);
        log.debug("创建字段级别断点: taskId={}, docId={}, field={}", taskId, docId, fieldName);
        
        return checkpoint;
    }
    
    /**
     * 创建批次级别断点
     */
    public FieldLevelCheckpoint createBatchCheckpoint(String taskId, long batchNumber, 
                                                     List<String> docIds, int batchSize) {
        FieldLevelCheckpoint checkpoint = new FieldLevelCheckpoint();
        checkpoint.setCheckpointId(generateCheckpointId());
        checkpoint.setTaskId(taskId);
        checkpoint.setType(FieldLevelCheckpoint.CheckpointType.BATCH_LEVEL);
        checkpoint.setCreateTime(new Date());
        checkpoint.setLastUpdateTime(new Date());
        checkpoint.setStatus(FieldLevelCheckpoint.CheckpointStatus.PENDING);
        
        FieldLevelCheckpoint.BatchCheckpoint batchCheckpoint = new FieldLevelCheckpoint.BatchCheckpoint();
        batchCheckpoint.setBatchNumber(batchNumber);
        batchCheckpoint.setBatchSize(batchSize);
        batchCheckpoint.setBatchDocIds(new ArrayList<>(docIds));
        batchCheckpoint.setProcessedInBatch(0);
        batchCheckpoint.setSuccessInBatch(0);
        batchCheckpoint.setFailedInBatch(0);
        batchCheckpoint.setBatchMetadata(new HashMap<>());
        
        checkpoint.setBatchCheckpoint(batchCheckpoint);
        
        saveCheckpoint(checkpoint);
        log.debug("创建批次级别断点: taskId={}, batchNumber={}, docCount={}", 
            taskId, batchNumber, docIds.size());
        
        return checkpoint;
    }
    
    /**
     * 创建时间范围断点
     */
    public FieldLevelCheckpoint createTimeRangeCheckpoint(String taskId, Date startTime, 
                                                         Date endTime, String timeField) {
        FieldLevelCheckpoint checkpoint = new FieldLevelCheckpoint();
        checkpoint.setCheckpointId(generateCheckpointId());
        checkpoint.setTaskId(taskId);
        checkpoint.setType(FieldLevelCheckpoint.CheckpointType.TIME_RANGE_LEVEL);
        checkpoint.setCreateTime(new Date());
        checkpoint.setLastUpdateTime(new Date());
        checkpoint.setStatus(FieldLevelCheckpoint.CheckpointStatus.PENDING);
        
        FieldLevelCheckpoint.TimeRangeCheckpoint timeRangeCheckpoint = new FieldLevelCheckpoint.TimeRangeCheckpoint();
        timeRangeCheckpoint.setStartTime(startTime);
        timeRangeCheckpoint.setEndTime(endTime);
        timeRangeCheckpoint.setCurrentTime(startTime);
        timeRangeCheckpoint.setTimeField(timeField);
        timeRangeCheckpoint.setTimeWindowMinutes(10); // 默认10分钟窗口
        timeRangeCheckpoint.setProcessedTimeRanges(0);
        timeRangeCheckpoint.setLastProcessedTime(startTime);
        
        checkpoint.setTimeRangeCheckpoint(timeRangeCheckpoint);
        
        saveCheckpoint(checkpoint);
        log.debug("创建时间范围断点: taskId={}, timeRange=[{} - {}]", taskId, startTime, endTime);
        
        return checkpoint;
    }
    
    /**
     * 更新字段级别断点状态
     */
    public void updateFieldCheckpoint(String checkpointId, String fieldName, 
                                     Object transformedValue, 
                                     FieldLevelCheckpoint.FieldCheckpoint.FieldProcessStatus status) {
        FieldLevelCheckpoint checkpoint = getCheckpointById(checkpointId);
        if (checkpoint != null && checkpoint.getFieldCheckpoints() != null) {
            FieldLevelCheckpoint.FieldCheckpoint fieldCheckpoint = checkpoint.getFieldCheckpoints().get(fieldName);
            if (fieldCheckpoint != null) {
                fieldCheckpoint.setTransformedValue(transformedValue);
                fieldCheckpoint.setStatus(status);
                checkpoint.updateTimestamp();
                
                log.debug("更新字段断点: checkpointId={}, field={}, status={}", 
                    checkpointId, fieldName, status);
            }
        }
    }
    
    /**
     * 更新批次断点进度
     */
    public void updateBatchProgress(String checkpointId, long processed, long success, long failed) {
        FieldLevelCheckpoint checkpoint = getCheckpointById(checkpointId);
        if (checkpoint != null && checkpoint.getBatchCheckpoint() != null) {
            FieldLevelCheckpoint.BatchCheckpoint batchCheckpoint = checkpoint.getBatchCheckpoint();
            batchCheckpoint.setProcessedInBatch(processed);
            batchCheckpoint.setSuccessInBatch(success);
            batchCheckpoint.setFailedInBatch(failed);
            checkpoint.updateTimestamp();
            
            if (processed >= batchCheckpoint.getBatchSize()) {
                checkpoint.setStatus(FieldLevelCheckpoint.CheckpointStatus.COMPLETED);
            }
            
            log.debug("更新批次断点进度: checkpointId={}, processed={}/{}", 
                checkpointId, processed, batchCheckpoint.getBatchSize());
        }
    }
    
    /**
     * 标记文档处理失败
     */
    public void markDocumentFailed(String taskId, String docId, String errorMessage) {
        List<FieldLevelCheckpoint> checkpoints = getCheckpoints(taskId);
        for (FieldLevelCheckpoint checkpoint : checkpoints) {
            if (checkpoint.getDocumentCheckpoint() != null) {
                checkpoint.getDocumentCheckpoint().getFailedDocIds().add(docId);
                checkpoint.setErrorMessage(errorMessage);
                checkpoint.updateTimestamp();
                
                log.warn("标记文档失败: taskId={}, docId={}, error={}", taskId, docId, errorMessage);
                break;
            }
        }
    }
    
    /**
     * 获取失败的文档列表
     */
    public Set<String> getFailedDocuments(String taskId) {
        List<FieldLevelCheckpoint> checkpoints = getCheckpoints(taskId);
        Set<String> failedDocs = new HashSet<>();
        
        for (FieldLevelCheckpoint checkpoint : checkpoints) {
            if (checkpoint.getDocumentCheckpoint() != null && 
                checkpoint.getDocumentCheckpoint().getFailedDocIds() != null) {
                failedDocs.addAll(checkpoint.getDocumentCheckpoint().getFailedDocIds());
            }
        }
        
        return failedDocs;
    }
    
    /**
     * 获取可恢复的断点
     */
    public List<FieldLevelCheckpoint> getRecoverableCheckpoints(String taskId) {
        List<FieldLevelCheckpoint> allCheckpoints = getCheckpoints(taskId);
        
        return allCheckpoints.stream()
            .filter(cp -> cp.getStatus() == FieldLevelCheckpoint.CheckpointStatus.FAILED 
                       || cp.getStatus() == FieldLevelCheckpoint.CheckpointStatus.SUSPENDED)
            .filter(FieldLevelCheckpoint::canRetry)
            .filter(cp -> !cp.isExpired(CHECKPOINT_EXPIRATION_MS))
            .sorted(Comparator.comparing(FieldLevelCheckpoint::getCreateTime))
            .collect(Collectors.toList());
    }
    
    /**
     * 从断点恢复处理
     */
    public ResumePlan createResumePlan(String taskId) {
        List<FieldLevelCheckpoint> recoverableCheckpoints = getRecoverableCheckpoints(taskId);
        
        ResumePlan plan = new ResumePlan();
        plan.setTaskId(taskId);
        plan.setTotalCheckpoints(recoverableCheckpoints.size());
        
        // 按类型分组
        Map<FieldLevelCheckpoint.CheckpointType, List<FieldLevelCheckpoint>> groupedCheckpoints = 
            recoverableCheckpoints.stream()
                .collect(Collectors.groupingBy(FieldLevelCheckpoint::getType));
        
        // 创建恢复步骤
        List<ResumeStep> steps = new ArrayList<>();
        
        // 1. 优先恢复文档级别的断点
        List<FieldLevelCheckpoint> docCheckpoints = groupedCheckpoints.get(FieldLevelCheckpoint.CheckpointType.DOCUMENT_LEVEL);
        if (docCheckpoints != null && !docCheckpoints.isEmpty()) {
            ResumeStep docStep = new ResumeStep();
            docStep.setStepType(ResumeStep.StepType.DOCUMENT_RECOVERY);
            docStep.setCheckpoints(docCheckpoints);
            docStep.setPriority(1);
            steps.add(docStep);
        }
        
        // 2. 恢复字段级别的断点
        List<FieldLevelCheckpoint> fieldCheckpoints = groupedCheckpoints.get(FieldLevelCheckpoint.CheckpointType.FIELD_LEVEL);
        if (fieldCheckpoints != null && !fieldCheckpoints.isEmpty()) {
            ResumeStep fieldStep = new ResumeStep();
            fieldStep.setStepType(ResumeStep.StepType.FIELD_RECOVERY);
            fieldStep.setCheckpoints(fieldCheckpoints);
            fieldStep.setPriority(2);
            steps.add(fieldStep);
        }
        
        // 3. 恢复批次级别的断点
        List<FieldLevelCheckpoint> batchCheckpoints = groupedCheckpoints.get(FieldLevelCheckpoint.CheckpointType.BATCH_LEVEL);
        if (batchCheckpoints != null && !batchCheckpoints.isEmpty()) {
            ResumeStep batchStep = new ResumeStep();
            batchStep.setStepType(ResumeStep.StepType.BATCH_RECOVERY);
            batchStep.setCheckpoints(batchCheckpoints);
            batchStep.setPriority(3);
            steps.add(batchStep);
        }
        
        // 4. 恢复时间范围断点
        List<FieldLevelCheckpoint> timeCheckpoints = groupedCheckpoints.get(FieldLevelCheckpoint.CheckpointType.TIME_RANGE_LEVEL);
        if (timeCheckpoints != null && !timeCheckpoints.isEmpty()) {
            ResumeStep timeStep = new ResumeStep();
            timeStep.setStepType(ResumeStep.StepType.TIME_RANGE_RECOVERY);
            timeStep.setCheckpoints(timeCheckpoints);
            timeStep.setPriority(4);
            steps.add(timeStep);
        }
        
        // 按优先级排序
        steps.sort(Comparator.comparingInt(ResumeStep::getPriority));
        plan.setSteps(steps);
        
        log.info("创建恢复计划: taskId={}, 总断点数={}, 恢复步骤数={}", 
            taskId, recoverableCheckpoints.size(), steps.size());
        
        return plan;
    }
    
    /**
     * 保存断点
     */
    private void saveCheckpoint(FieldLevelCheckpoint checkpoint) {
        checkpointCache.computeIfAbsent(checkpoint.getTaskId(), k -> new ArrayList<>()).add(checkpoint);
        
        // 限制每个任务的断点数量
        List<FieldLevelCheckpoint> taskCheckpoints = checkpointCache.get(checkpoint.getTaskId());
        if (taskCheckpoints.size() > MAX_CHECKPOINTS_PER_TASK) {
            // 移除最旧的断点
            taskCheckpoints.sort(Comparator.comparing(FieldLevelCheckpoint::getCreateTime));
            taskCheckpoints.subList(0, taskCheckpoints.size() - MAX_CHECKPOINTS_PER_TASK).clear();
            log.debug("清理过期断点，保留最新{}个断点", MAX_CHECKPOINTS_PER_TASK);
        }
    }
    
    /**
     * 获取断点列表
     */
    private List<FieldLevelCheckpoint> getCheckpoints(String taskId) {
        return checkpointCache.getOrDefault(taskId, new ArrayList<>());
    }
    
    /**
     * 根据ID获取断点
     */
    private FieldLevelCheckpoint getCheckpointById(String checkpointId) {
        return checkpointCache.values().stream()
            .flatMap(List::stream)
            .filter(cp -> checkpointId.equals(cp.getCheckpointId()))
            .findFirst()
            .orElse(null);
    }
    
    /**
     * 生成断点ID
     */
    private String generateCheckpointId() {
        return "checkpoint_" + System.currentTimeMillis() + "_" + checkpointSequence.getAndIncrement();
    }
    
    /**
     * 清理过期断点
     */
    public void cleanupExpiredCheckpoints() {
        int totalCleaned = 0;
        
        for (Map.Entry<String, List<FieldLevelCheckpoint>> entry : checkpointCache.entrySet()) {
            List<FieldLevelCheckpoint> checkpoints = entry.getValue();
            int sizeBefore = checkpoints.size();
            
            checkpoints.removeIf(cp -> cp.isExpired(CHECKPOINT_EXPIRATION_MS));
            
            int cleaned = sizeBefore - checkpoints.size();
            totalCleaned += cleaned;
            
            if (cleaned > 0) {
                log.debug("清理任务{}的过期断点{}个", entry.getKey(), cleaned);
            }
        }
        
        if (totalCleaned > 0) {
            log.info("清理过期断点完成，共清理{}个断点", totalCleaned);
        }
    }
    
    /**
     * 获取断点统计信息
     */
    public CheckpointStatistics getStatistics(String taskId) {
        List<FieldLevelCheckpoint> checkpoints = getCheckpoints(taskId);
        
        CheckpointStatistics stats = new CheckpointStatistics();
        stats.setTaskId(taskId);
        stats.setTotalCheckpoints(checkpoints.size());
        
        Map<FieldLevelCheckpoint.CheckpointStatus, Long> statusCount = checkpoints.stream()
            .collect(Collectors.groupingBy(FieldLevelCheckpoint::getStatus, Collectors.counting()));
        
        stats.setPendingCount(statusCount.getOrDefault(FieldLevelCheckpoint.CheckpointStatus.PENDING, 0L).intValue());
        stats.setProcessingCount(statusCount.getOrDefault(FieldLevelCheckpoint.CheckpointStatus.PROCESSING, 0L).intValue());
        stats.setCompletedCount(statusCount.getOrDefault(FieldLevelCheckpoint.CheckpointStatus.COMPLETED, 0L).intValue());
        stats.setFailedCount(statusCount.getOrDefault(FieldLevelCheckpoint.CheckpointStatus.FAILED, 0L).intValue());
        stats.setSuspendedCount(statusCount.getOrDefault(FieldLevelCheckpoint.CheckpointStatus.SUSPENDED, 0L).intValue());
        
        Map<FieldLevelCheckpoint.CheckpointType, Long> typeCount = checkpoints.stream()
            .collect(Collectors.groupingBy(FieldLevelCheckpoint::getType, Collectors.counting()));
        
        stats.setDocumentLevelCount(typeCount.getOrDefault(FieldLevelCheckpoint.CheckpointType.DOCUMENT_LEVEL, 0L).intValue());
        stats.setFieldLevelCount(typeCount.getOrDefault(FieldLevelCheckpoint.CheckpointType.FIELD_LEVEL, 0L).intValue());
        stats.setBatchLevelCount(typeCount.getOrDefault(FieldLevelCheckpoint.CheckpointType.BATCH_LEVEL, 0L).intValue());
        stats.setTimeRangeLevelCount(typeCount.getOrDefault(FieldLevelCheckpoint.CheckpointType.TIME_RANGE_LEVEL, 0L).intValue());
        
        return stats;
    }
    
    /**
     * 断点统计信息
     */
    @lombok.Data
    public static class CheckpointStatistics {
        private String taskId;
        private int totalCheckpoints;
        private int pendingCount;
        private int processingCount;
        private int completedCount;
        private int failedCount;
        private int suspendedCount;
        private int documentLevelCount;
        private int fieldLevelCount;
        private int batchLevelCount;
        private int timeRangeLevelCount;
        
        public double getSuccessRate() {
            return totalCheckpoints > 0 ? (double) completedCount / totalCheckpoints * 100.0 : 0.0;
        }
        
        public double getFailureRate() {
            return totalCheckpoints > 0 ? (double) failedCount / totalCheckpoints * 100.0 : 0.0;
        }
    }
}