package com.everflowx.esmigration.service;

import com.everflowx.esmigration.domain.MigrationConfig;
import com.everflowx.esmigration.manager.MigrationTaskManager;
import com.everflowx.esmigration.service.EsMigrationService.MigrationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;

/**
 * 增强版迁移服务
 * 支持任务进度回调和实时监控
 * 
 * @author everflowx
 */
@Slf4j
@Service
public class EnhancedMigrationService {
    
    @Autowired
    private EsMigrationService migrationService;
    
    @Autowired
    private MigrationTaskManager taskManager;
    
    /**
     * 带进度回调的全量迁移
     */
    public MigrationResult fullMigrationWithCallback(String taskId, MigrationConfig config) {
        try {
            // 更新任务状态为开始
            taskManager.updateTaskProgress(taskId, 0, 0, 0, 0, "初始化迁移任务");
            
            // 检查源索引是否存在
            if (!migrationService.indexExists(config.getSourceIndex(), false)) {
                String errorMsg = "源索引不存在: " + config.getSourceIndex();
                taskManager.updateTaskProgress(taskId, 0, 0, 0, 0, errorMsg);
                MigrationResult result = new MigrationResult();
                result.setErrorMessage(errorMsg);
                result.finish();
                return result;
            }
            
            // 获取总文档数
            taskManager.updateTaskProgress(taskId, 0, 0, 0, 0, "获取源索引文档数量");
            long totalCount = migrationService.getDocumentCount(config.getSourceIndex(), false);
            taskManager.updateTaskProgress(taskId, totalCount, 0, 0, 0, "开始数据迁移");
            
            log.info("任务 {} 开始全量迁移，总文档数: {}", taskId, totalCount);
            
            // 执行迁移（这里应该是一个增强版的迁移方法，支持进度回调）
            MigrationResult result = migrationService.fullMigration(config);
            
            // 更新最终进度
            taskManager.updateTaskProgress(taskId, 
                result.getTotalCount(), 
                result.getSuccessCount() + result.getFailedCount(),
                result.getSuccessCount(), 
                result.getFailedCount(), 
                result.isSuccess() ? "迁移完成" : "迁移失败");
            
            return result;
            
        } catch (Exception e) {
            log.error("任务 {} 全量迁移失败", taskId, e);
            taskManager.updateTaskProgress(taskId, 0, 0, 0, 0, "迁移异常: " + e.getMessage());
            
            MigrationResult result = new MigrationResult();
            result.setErrorMessage("迁移异常: " + e.getMessage());
            result.finish();
            return result;
        }
    }
    
    /**
     * 带进度回调的增量迁移
     */
    public MigrationResult incrementalMigrationWithCallback(String taskId, MigrationConfig config, Date lastSyncTime) {
        try {
            taskManager.updateTaskProgress(taskId, 0, 0, 0, 0, "初始化增量迁移任务");
            
            // 检查源索引是否存在
            if (!migrationService.indexExists(config.getSourceIndex(), false)) {
                String errorMsg = "源索引不存在: " + config.getSourceIndex();
                taskManager.updateTaskProgress(taskId, 0, 0, 0, 0, errorMsg);
                MigrationResult result = new MigrationResult();
                result.setErrorMessage(errorMsg);
                result.finish();
                return result;
            }
            
            taskManager.updateTaskProgress(taskId, 0, 0, 0, 0, "开始增量迁移");
            
            log.info("任务 {} 开始增量迁移，上次同步时间: {}", taskId, lastSyncTime);
            
            // 执行增量迁移
            MigrationResult result = migrationService.incrementalMigration(config, lastSyncTime);
            
            // 更新最终进度
            taskManager.updateTaskProgress(taskId, 
                result.getTotalCount(), 
                result.getSuccessCount() + result.getFailedCount(),
                result.getSuccessCount(), 
                result.getFailedCount(), 
                result.isSuccess() ? "增量迁移完成" : "增量迁移失败");
            
            return result;
            
        } catch (Exception e) {
            log.error("任务 {} 增量迁移失败", taskId, e);
            taskManager.updateTaskProgress(taskId, 0, 0, 0, 0, "增量迁移异常: " + e.getMessage());
            
            MigrationResult result = new MigrationResult();
            result.setErrorMessage("增量迁移异常: " + e.getMessage());
            result.finish();
            return result;
        }
    }
    
    /**
     * 模拟进度更新的演示方法
     * 实际项目中应该在批处理过程中调用taskManager.updateTaskProgress
     */
    public void simulateProgressUpdate(String taskId, long totalDocs) {
        new Thread(() -> {
            try {
                long batchSize = 1000;
                long processed = 0;
                long success = 0;
                long failed = 0;
                
                while (processed < totalDocs) {
                    Thread.sleep(2000); // 模拟处理时间
                    
                    long currentBatch = Math.min(batchSize, totalDocs - processed);
                    processed += currentBatch;
                    success += (long)(currentBatch * 0.95); // 95%成功率
                    failed += currentBatch - (long)(currentBatch * 0.95);
                    
                    String phase = String.format("处理批次 %d/%d", 
                        processed / batchSize + 1, 
                        (totalDocs + batchSize - 1) / batchSize);
                    
                    taskManager.updateTaskProgress(taskId, totalDocs, processed, success, failed, phase);
                    taskManager.updateBatchProgress(taskId, processed / batchSize + 1, (totalDocs + batchSize - 1) / batchSize);
                    
                    // 模拟处理速度
                    long speed = currentBatch / 2; // 文档/秒
                    taskManager.updateTaskSpeed(taskId, speed);
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("进度更新线程被中断");
            }
        }).start();
    }
}