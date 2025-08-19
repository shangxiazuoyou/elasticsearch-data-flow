package com.everflowx.esmigration.checkpoint;

import com.alibaba.fastjson.JSON;
import com.everflowx.esmigration.domain.MigrationConfig;
import com.everflowx.esmigration.service.EsMigrationService;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 字段级别恢复引擎
 * 负责执行细粒度的断点恢复操作
 * 
 * @author everflowx
 */
@Slf4j
@Component
public class FieldLevelRecoveryEngine {
    
    @Resource
    private AdvancedCheckpointManager checkpointManager;
    
    @Resource
    private EsMigrationService migrationService;
    
    // 恢复线程池
    private final ExecutorService recoveryExecutor = Executors.newFixedThreadPool(4);
    
    // 最大批次大小
    private static final int MAX_RECOVERY_BATCH_SIZE = 100;
    
    // 重试间隔（毫秒）
    private static final long RETRY_DELAY_MS = 1000;
    
    /**
     * 执行恢复计划
     */
    public CompletableFuture<RecoveryResult> executeRecoveryPlan(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config,
            ResumePlan plan) {
        
        return CompletableFuture.supplyAsync(() -> {
            RecoveryResult result = new RecoveryResult();
            result.setTaskId(plan.getTaskId());
            result.setStartTime(new Date());
            
            try {
                log.info("开始执行恢复计划: taskId={}, 步骤数={}", 
                    plan.getTaskId(), plan.getSteps().size());
                
                plan.start();
                
                // 按优先级顺序执行每个步骤
                for (ResumeStep step : plan.getSteps()) {
                    if (plan.getStatus() == ResumePlan.PlanStatus.CANCELLED) {
                        log.info("恢复计划被取消: {}", plan.getTaskId());
                        break;
                    }
                    
                    StepResult stepResult = executeRecoveryStep(
                        sourceClient, targetClient, config, step);
                    
                    result.getStepResults().add(stepResult);
                    result.updateFromStepResult(stepResult);
                    
                    if (!stepResult.isSuccess() && step.getStepType().name().contains("CRITICAL")) {
                        // 关键步骤失败，终止恢复
                        log.error("关键恢复步骤失败，终止恢复计划: {}", stepResult.getErrorMessage());
                        plan.fail("关键步骤失败: " + stepResult.getErrorMessage());
                        break;
                    }
                    
                    plan.moveToNextStep();
                }
                
                if (plan.isCompleted() && plan.getStatus() == ResumePlan.PlanStatus.EXECUTING) {
                    plan.complete();
                    result.setSuccess(true);
                    log.info("恢复计划执行完成: taskId={}", plan.getTaskId());
                } else if (plan.getStatus() == ResumePlan.PlanStatus.FAILED) {
                    result.setSuccess(false);
                    result.setErrorMessage(plan.getErrorMessage());
                }
                
            } catch (Exception e) {
                log.error("执行恢复计划异常: taskId=" + plan.getTaskId(), e);
                plan.fail("执行异常: " + e.getMessage());
                result.setSuccess(false);
                result.setErrorMessage(e.getMessage());
            }
            
            result.setEndTime(new Date());
            result.setDurationMs(result.getEndTime().getTime() - result.getStartTime().getTime());
            
            log.info("恢复计划执行完成: taskId={}, 成功={}, 耗时={}ms, 总断点={}, 成功={}, 失败={}", 
                plan.getTaskId(), result.isSuccess(), result.getDurationMs(),
                result.getTotalCheckpoints(), result.getSuccessfulCheckpoints(), result.getFailedCheckpoints());
            
            return result;
            
        }, recoveryExecutor);
    }
    
    /**
     * 执行单个恢复步骤
     */
    private StepResult executeRecoveryStep(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config,
            ResumeStep step) {
        
        StepResult result = new StepResult();
        result.setStepId(step.getStepId());
        result.setStepType(step.getStepType());
        result.setStartTime(new Date());
        
        try {
            log.info("开始执行恢复步骤: type={}, 断点数={}", 
                step.getStepType(), step.getCheckpoints().size());
            
            step.start();
            
            switch (step.getStepType()) {
                case DOCUMENT_RECOVERY:
                    executeDocumentRecovery(sourceClient, targetClient, config, step, result);
                    break;
                case FIELD_RECOVERY:
                    executeFieldRecovery(sourceClient, targetClient, config, step, result);
                    break;
                case BATCH_RECOVERY:
                    executeBatchRecovery(sourceClient, targetClient, config, step, result);
                    break;
                case TIME_RANGE_RECOVERY:
                    executeTimeRangeRecovery(sourceClient, targetClient, config, step, result);
                    break;
                default:
                    log.warn("未支持的恢复步骤类型: {}", step.getStepType());
                    step.skip("未支持的步骤类型");
            }
            
            if (step.getStatus() == ResumeStep.StepStatus.COMPLETED) {
                result.setSuccess(true);
                step.complete();
            } else if (step.getStatus() == ResumeStep.StepStatus.FAILED) {
                result.setSuccess(false);
                result.setErrorMessage(step.getErrorMessage());
            }
            
        } catch (Exception e) {
            log.error("执行恢复步骤异常: type=" + step.getStepType(), e);
            step.fail("执行异常: " + e.getMessage());
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
        }
        
        result.setEndTime(new Date());
        result.setDurationMs(result.getEndTime().getTime() - result.getStartTime().getTime());
        result.setProcessedCheckpoints(step.getProcessedCheckpoints());
        result.setSuccessfulCheckpoints(step.getSuccessfulCheckpoints());
        result.setFailedCheckpoints(step.getFailedCheckpoints());
        
        return result;
    }
    
    /**
     * 执行文档级别恢复
     */
    private void executeDocumentRecovery(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config,
            ResumeStep step,
            StepResult result) throws IOException {
        
        log.info("执行文档级别恢复，断点数: {}", step.getCheckpoints().size());
        
        for (FieldLevelCheckpoint checkpoint : step.getCheckpoints()) {
            if (checkpoint.getDocumentCheckpoint() == null) {
                continue;
            }
            
            FieldLevelCheckpoint.DocumentCheckpoint docCheckpoint = checkpoint.getDocumentCheckpoint();
            Set<String> failedDocIds = docCheckpoint.getFailedDocIds();
            
            if (failedDocIds == null || failedDocIds.isEmpty()) {
                step.incrementSuccess();
                continue;
            }
            
            // 批量恢复失败的文档
            List<String> docIdList = new ArrayList<>(failedDocIds);
            for (int i = 0; i < docIdList.size(); i += MAX_RECOVERY_BATCH_SIZE) {
                int endIndex = Math.min(i + MAX_RECOVERY_BATCH_SIZE, docIdList.size());
                List<String> batchDocIds = docIdList.subList(i, endIndex);
                
                try {
                    recoverDocumentBatch(sourceClient, targetClient, config, batchDocIds);
                    
                    // 从失败列表中移除成功恢复的文档
                    failedDocIds.removeAll(batchDocIds);
                    step.incrementSuccess();
                    
                    log.debug("文档恢复批次成功: 恢复{}个文档", batchDocIds.size());
                    
                } catch (Exception e) {
                    log.error("文档恢复批次失败: docIds=" + batchDocIds, e);
                    step.incrementFailure();
                    
                    // 增加重试次数
                    checkpoint.incrementRetry();
                    if (!checkpoint.canRetry()) {
                        checkpoint.setStatus(FieldLevelCheckpoint.CheckpointStatus.FAILED);
                        checkpoint.setErrorMessage("重试次数超限: " + e.getMessage());
                    }
                }
            }
        }
    }
    
    /**
     * 执行字段级别恢复
     */
    private void executeFieldRecovery(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config,
            ResumeStep step,
            StepResult result) throws IOException {
        
        log.info("执行字段级别恢复，断点数: {}", step.getCheckpoints().size());
        
        for (FieldLevelCheckpoint checkpoint : step.getCheckpoints()) {
            if (checkpoint.getFieldCheckpoints() == null) {
                continue;
            }
            
            for (Map.Entry<String, FieldLevelCheckpoint.FieldCheckpoint> entry : 
                 checkpoint.getFieldCheckpoints().entrySet()) {
                
                FieldLevelCheckpoint.FieldCheckpoint fieldCheckpoint = entry.getValue();
                
                if (fieldCheckpoint.getStatus() == FieldLevelCheckpoint.FieldCheckpoint.FieldProcessStatus.FAILED) {
                    try {
                        // 恢复单个字段
                        recoverSingleField(sourceClient, targetClient, config, fieldCheckpoint);
                        
                        fieldCheckpoint.setStatus(FieldLevelCheckpoint.FieldCheckpoint.FieldProcessStatus.INDEXED);
                        step.incrementSuccess();
                        
                        log.debug("字段恢复成功: docId={}, field={}", 
                            fieldCheckpoint.getDocumentId(), fieldCheckpoint.getFieldName());
                        
                    } catch (Exception e) {
                        log.error("字段恢复失败: docId={}, field={}", 
                            fieldCheckpoint.getDocumentId(), fieldCheckpoint.getFieldName(), e);
                        
                        fieldCheckpoint.setRetryCount(fieldCheckpoint.getRetryCount() + 1);
                        fieldCheckpoint.setErrorMessage(e.getMessage());
                        step.incrementFailure();
                    }
                }
            }
        }
    }
    
    /**
     * 执行批次级别恢复
     */
    private void executeBatchRecovery(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config,
            ResumeStep step,
            StepResult result) throws IOException {
        
        log.info("执行批次级别恢复，断点数: {}", step.getCheckpoints().size());
        
        for (FieldLevelCheckpoint checkpoint : step.getCheckpoints()) {
            if (checkpoint.getBatchCheckpoint() == null) {
                continue;
            }
            
            FieldLevelCheckpoint.BatchCheckpoint batchCheckpoint = checkpoint.getBatchCheckpoint();
            List<String> batchDocIds = batchCheckpoint.getBatchDocIds();
            
            if (batchDocIds == null || batchDocIds.isEmpty()) {
                step.incrementSuccess();
                continue;
            }
            
            try {
                // 重新处理整个批次
                recoverDocumentBatch(sourceClient, targetClient, config, batchDocIds);
                
                // 更新批次状态
                batchCheckpoint.setProcessedInBatch(batchDocIds.size());
                batchCheckpoint.setSuccessInBatch(batchDocIds.size());
                batchCheckpoint.setFailedInBatch(0);
                
                checkpoint.setStatus(FieldLevelCheckpoint.CheckpointStatus.COMPLETED);
                step.incrementSuccess();
                
                log.debug("批次恢复成功: batchNumber={}, docCount={}", 
                    batchCheckpoint.getBatchNumber(), batchDocIds.size());
                
            } catch (Exception e) {
                log.error("批次恢复失败: batchNumber=" + batchCheckpoint.getBatchNumber(), e);
                
                batchCheckpoint.setFailedInBatch(batchDocIds.size());
                checkpoint.incrementRetry();
                step.incrementFailure();
            }
        }
    }
    
    /**
     * 执行时间范围恢复
     */
    private void executeTimeRangeRecovery(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config,
            ResumeStep step,
            StepResult result) throws IOException {
        
        log.info("执行时间范围恢复，断点数: {}", step.getCheckpoints().size());
        
        for (FieldLevelCheckpoint checkpoint : step.getCheckpoints()) {
            if (checkpoint.getTimeRangeCheckpoint() == null) {
                continue;
            }
            
            FieldLevelCheckpoint.TimeRangeCheckpoint timeCheckpoint = checkpoint.getTimeRangeCheckpoint();
            
            try {
                // 恢复时间范围内的数据
                Date startTime = timeCheckpoint.getCurrentTime();
                Date endTime = timeCheckpoint.getEndTime();
                String timeField = timeCheckpoint.getTimeField();
                
                if (startTime != null && endTime != null && startTime.before(endTime)) {
                    recoverTimeRangeData(sourceClient, targetClient, config, 
                        startTime, endTime, timeField);
                    
                    // 更新时间范围状态
                    timeCheckpoint.setCurrentTime(endTime);
                    timeCheckpoint.setProcessedTimeRanges(timeCheckpoint.getProcessedTimeRanges() + 1);
                    timeCheckpoint.setLastProcessedTime(endTime);
                    
                    checkpoint.setStatus(FieldLevelCheckpoint.CheckpointStatus.COMPLETED);
                    step.incrementSuccess();
                    
                    log.debug("时间范围恢复成功: timeRange=[{} - {}]", startTime, endTime);
                } else {
                    step.incrementSuccess(); // 无效时间范围，直接跳过
                }
                
            } catch (Exception e) {
                log.error("时间范围恢复失败", e);
                checkpoint.incrementRetry();
                step.incrementFailure();
            }
        }
    }
    
    /**
     * 恢复文档批次
     */
    private void recoverDocumentBatch(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config,
            List<String> docIds) throws IOException {
        
        if (docIds == null || docIds.isEmpty()) {
            return;
        }
        
        BulkRequest bulkRequest = new BulkRequest();
        
        // 从源ES获取文档
        for (String docId : docIds) {
            try {
                GetRequest getRequest = new GetRequest(config.getSourceIndex(), docId);
                GetResponse getResponse = sourceClient.get(getRequest, RequestOptions.DEFAULT);
                
                if (getResponse.isExists()) {
                    Map<String, Object> sourceData = getResponse.getSourceAsMap();
                    Map<String, Object> transformedData = transformDocument(sourceData, config);
                    
                    IndexRequest indexRequest = new IndexRequest(config.getTargetIndex())
                        .id(docId)
                        .source(transformedData, XContentType.JSON);
                    
                    bulkRequest.add(indexRequest);
                } else {
                    log.warn("恢复时源文档不存在: docId={}", docId);
                }
            } catch (Exception e) {
                log.error("获取源文档失败: docId=" + docId, e);
            }
        }
        
        // 批量写入目标ES
        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkResponse = targetClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            
            if (bulkResponse.hasFailures()) {
                log.warn("文档批次恢复部分失败: {}", bulkResponse.buildFailureMessage());
                throw new IOException("批次恢复部分失败");
            }
        }
    }
    
    /**
     * 恢复单个字段
     */
    private void recoverSingleField(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config,
            FieldLevelCheckpoint.FieldCheckpoint fieldCheckpoint) throws IOException {
        
        String docId = fieldCheckpoint.getDocumentId();
        String fieldName = fieldCheckpoint.getFieldName();
        
        // 获取源文档
        GetRequest sourceGetRequest = new GetRequest(config.getSourceIndex(), docId);
        GetResponse sourceGetResponse = sourceClient.get(sourceGetRequest, RequestOptions.DEFAULT);
        
        if (!sourceGetResponse.isExists()) {
            throw new IOException("源文档不存在: " + docId);
        }
        
        // 获取目标文档
        GetRequest targetGetRequest = new GetRequest(config.getTargetIndex(), docId);
        GetResponse targetGetResponse = targetClient.get(targetGetRequest, RequestOptions.DEFAULT);
        
        Map<String, Object> targetData;
        if (targetGetResponse.isExists()) {
            targetData = new HashMap<>(targetGetResponse.getSourceAsMap());
        } else {
            targetData = new HashMap<>();
        }
        
        // 转换并更新特定字段
        Map<String, Object> sourceData = sourceGetResponse.getSourceAsMap();
        Object fieldValue = sourceData.get(fieldName);
        
        if (fieldValue != null) {
            Object transformedValue = transformFieldValue(fieldValue, fieldName, config);
            targetData.put(fieldName, transformedValue);
            
            // 更新目标文档
            IndexRequest indexRequest = new IndexRequest(config.getTargetIndex())
                .id(docId)
                .source(targetData, XContentType.JSON);
            
            targetClient.index(indexRequest, RequestOptions.DEFAULT);
            
            // 更新字段检查点
            fieldCheckpoint.setTransformedValue(transformedValue);
        }
    }
    
    /**
     * 恢复时间范围数据
     */
    private void recoverTimeRangeData(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config,
            Date startTime,
            Date endTime,
            String timeField) throws IOException {
        
        SearchRequest searchRequest = new SearchRequest(config.getSourceIndex());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        
        // 构建时间范围查询
        sourceBuilder.query(QueryBuilders.rangeQuery(timeField)
            .gte(startTime.getTime())
            .lt(endTime.getTime()));
        
        sourceBuilder.size(1000); // 批量处理
        searchRequest.source(sourceBuilder);
        
        SearchResponse searchResponse = sourceClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        
        if (hits.length > 0) {
            List<String> docIds = Arrays.stream(hits)
                .map(SearchHit::getId)
                .collect(Collectors.toList());
            
            recoverDocumentBatch(sourceClient, targetClient, config, docIds);
        }
    }
    
    /**
     * 转换文档数据
     */
    private Map<String, Object> transformDocument(Map<String, Object> sourceData, MigrationConfig config) {
        // 这里应该复用原有的文档转换逻辑
        // 为简化，直接返回源数据
        return new HashMap<>(sourceData);
    }
    
    /**
     * 转换字段值
     */
    private Object transformFieldValue(Object fieldValue, String fieldName, MigrationConfig config) {
        // 实现字段级别的转换逻辑
        // 这里可以根据配置进行具体的字段转换
        return fieldValue;
    }
    
    /**
     * 恢复结果
     */
    @lombok.Data
    public static class RecoveryResult {
        private String taskId;
        private boolean success;
        private Date startTime;
        private Date endTime;
        private long durationMs;
        private int totalCheckpoints;
        private int successfulCheckpoints;
        private int failedCheckpoints;
        private String errorMessage;
        private List<StepResult> stepResults = new ArrayList<>();
        
        public void updateFromStepResult(StepResult stepResult) {
            totalCheckpoints += stepResult.getProcessedCheckpoints();
            successfulCheckpoints += stepResult.getSuccessfulCheckpoints();
            failedCheckpoints += stepResult.getFailedCheckpoints();
        }
        
        public double getSuccessRate() {
            return totalCheckpoints > 0 ? (double) successfulCheckpoints / totalCheckpoints * 100.0 : 0.0;
        }
    }
    
    /**
     * 步骤结果
     */
    @lombok.Data
    public static class StepResult {
        private String stepId;
        private ResumeStep.StepType stepType;
        private boolean success;
        private Date startTime;
        private Date endTime;
        private long durationMs;
        private int processedCheckpoints;
        private int successfulCheckpoints;
        private int failedCheckpoints;
        private String errorMessage;
        
        public double getSuccessRate() {
            return processedCheckpoints > 0 ? (double) successfulCheckpoints / processedCheckpoints * 100.0 : 0.0;
        }
    }
}