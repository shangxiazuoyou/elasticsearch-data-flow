package com.everflowx.esmigration.task;

import com.everflowx.esmigration.domain.MigrationConfig;
import com.everflowx.esmigration.service.EsMigrationService;
import com.everflowx.esmigration.service.EsMigrationService.MigrationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * ES数据迁移定时任务
 * 
 * @author everflowx
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "es.migration.task.enabled", havingValue = "true", matchIfMissing = false)
public class EsMigrationTask {
    
    @Autowired
    private EsMigrationService esMigrationService;
    
    @Value("${es.migration.task.source-index:}")
    private String sourceIndex;
    
    @Value("${es.migration.task.target-index:}")
    private String targetIndex;
    
    @Value("${es.migration.task.batch-size:1000}")
    private Integer batchSize;
    
    @Value("${es.migration.task.timestamp-field:updateTime}")
    private String timestampField;
    
    @Value("${es.migration.task.case-insensitive-fields:}")
    private String caseInsensitiveFieldsStr;
    
    @Value("${es.migration.task.new-fields:}")
    private String newFieldsStr;
    
    @Value("${es.migration.task.field-mapping:}")
    private String fieldMappingStr;
    
    private MigrationConfig migrationConfig;
    
    @PostConstruct
    public void init() {
        if (sourceIndex.isEmpty() || targetIndex.isEmpty()) {
            log.warn("ES迁移任务配置不完整，任务将不会执行");
            return;
        }
        
        migrationConfig = new MigrationConfig();
        migrationConfig.setSourceIndex(sourceIndex);
        migrationConfig.setTargetIndex(targetIndex);
        migrationConfig.setBatchSize(batchSize);
        migrationConfig.setTimestampField(timestampField);
        migrationConfig.setIncrementalSync(true);
        migrationConfig.setOverwriteExisting(true);
        
        // 解析忽略大小写字段
        if (!caseInsensitiveFieldsStr.isEmpty()) {
            List<String> caseInsensitiveFields = Arrays.asList(caseInsensitiveFieldsStr.split(","));
            migrationConfig.setCaseInsensitiveFields(caseInsensitiveFields);
        }
        
        // 解析新增字段
        if (!newFieldsStr.isEmpty()) {
            Map<String, Object> newFields = parseKeyValuePairs(newFieldsStr);
            migrationConfig.setNewFields(newFields);
        }
        
        // 解析字段映射
        if (!fieldMappingStr.isEmpty()) {
            Map<String, String> fieldMapping = parseFieldMapping(fieldMappingStr);
            migrationConfig.setFieldMapping(fieldMapping);
        }
        
        log.info("ES迁移任务初始化完成: {}", migrationConfig);
    }
    
    /**
     * 增量同步任务 - 每5分钟执行一次
     */
    @Scheduled(fixedRate = 300000) // 5分钟
    public void incrementalSync() {
        if (migrationConfig == null) {
            return;
        }
        
        try {
            log.info("开始执行增量同步任务");
            
            // 获取上次同步时间
            Date lastSyncTime = esMigrationService.getLastSyncTime(targetIndex, timestampField);
            
            // 执行增量迁移
            MigrationResult result = esMigrationService.incrementalMigration(migrationConfig, lastSyncTime);
            
            if (result.getSuccessCount() > 0) {
                log.info("增量同步完成，成功: {}, 失败: {}, 耗时: {}ms", 
                        result.getSuccessCount(), result.getFailedCount(), result.getDurationMs());
            } else {
                log.debug("增量同步完成，无新数据");
            }
            
            if (result.getFailedCount() > 0) {
                log.warn("增量同步存在失败记录: {}, 错误信息: {}", 
                        result.getFailedCount(), result.getErrorMessage());
            }
            
        } catch (Exception e) {
            log.error("增量同步任务执行失败", e);
        }
    }
    
    /**
     * 全量同步任务 - 每天凌晨2点执行
     */
    @Scheduled(cron = "0 0 2 * * ?")
    public void fullSync() {
        if (migrationConfig == null) {
            return;
        }
        
        try {
            log.info("开始执行全量同步任务");
            
            // 创建全量同步配置
            MigrationConfig fullConfig = new MigrationConfig();
            fullConfig.setSourceIndex(migrationConfig.getSourceIndex());
            fullConfig.setTargetIndex(migrationConfig.getTargetIndex());
            fullConfig.setBatchSize(migrationConfig.getBatchSize());
            fullConfig.setTimestampField(migrationConfig.getTimestampField());
            fullConfig.setCaseInsensitiveFields(migrationConfig.getCaseInsensitiveFields());
            fullConfig.setNewFields(migrationConfig.getNewFields());
            fullConfig.setFieldMapping(migrationConfig.getFieldMapping());
            fullConfig.setIncrementalSync(false);
            fullConfig.setOverwriteExisting(true);
            
            // 执行全量迁移
            MigrationResult result = esMigrationService.fullMigration(fullConfig);
            
            log.info("全量同步完成，总数: {}, 成功: {}, 失败: {}, 耗时: {}ms", 
                    result.getTotalCount(), result.getSuccessCount(), 
                    result.getFailedCount(), result.getDurationMs());
            
            if (result.getFailedCount() > 0) {
                log.error("全量同步存在失败记录: {}, 错误信息: {}", 
                        result.getFailedCount(), result.getErrorMessage());
            }
            
            // 验证迁移结果
            EsMigrationService.ValidationResult validation = esMigrationService.validateMigration(fullConfig);
            if (validation.isSuccess()) {
                log.info("全量同步验证通过");
            } else {
                log.error("全量同步验证失败: {}", validation.getMessage());
            }
            
        } catch (Exception e) {
            log.error("全量同步任务执行失败", e);
        }
    }
    
    /**
     * 解析键值对字符串
     * 格式: key1=value1,key2=value2
     */
    private Map<String, Object> parseKeyValuePairs(String str) {
        Map<String, Object> result = new HashMap<>();
        if (str != null && !str.trim().isEmpty()) {
            String[] pairs = str.split(",");
            for (String pair : pairs) {
                String[] kv = pair.split("=", 2);
                if (kv.length == 2) {
                    String key = kv[0].trim();
                    String value = kv[1].trim();
                    
                    // 尝试转换为合适的类型
                    Object convertedValue = convertValue(value);
                    result.put(key, convertedValue);
                }
            }
        }
        return result;
    }
    
    /**
     * 解析字段映射字符串
     * 格式: sourceField1=targetField1,sourceField2=targetField2
     */
    private Map<String, String> parseFieldMapping(String str) {
        Map<String, String> result = new HashMap<>();
        if (str != null && !str.trim().isEmpty()) {
            String[] pairs = str.split(",");
            for (String pair : pairs) {
                String[] kv = pair.split("=", 2);
                if (kv.length == 2) {
                    result.put(kv[0].trim(), kv[1].trim());
                }
            }
        }
        return result;
    }
    
    /**
     * 转换值类型
     */
    private Object convertValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            return value;
        }
        
        value = value.trim();
        
        // 布尔值
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.parseBoolean(value);
        }
        
        // 数字
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            } else {
                return Long.parseLong(value);
            }
        } catch (NumberFormatException e) {
            // 不是数字，返回字符串
        }
        
        // 日期（当前时间）
        if ("now".equalsIgnoreCase(value)) {
            return new Date();
        }
        
        return value;
    }
}
