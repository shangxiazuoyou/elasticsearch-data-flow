package com.everflowx.esmigration.controller;

import com.everflowx.esmigration.domain.MigrationConfig;
import com.everflowx.esmigration.service.EsMigrationService;
import com.everflowx.esmigration.service.EsMigrationService.MigrationResult;
import com.everflowx.esmigration.service.EsMigrationService.ValidationResult;
import com.everflowx.esmigration.service.impl.EsMigrationServiceImpl;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * ES数据迁移控制器
 * 
 * @author everflowx
 */
@Api(tags = "ES数据迁移")
@RestController
@RequestMapping("/api/es-migration")
@Slf4j
public class EsMigrationController {

    @Autowired
    private EsMigrationService esMigrationService;

    @ApiOperation("全量数据迁移")
    @PostMapping("/full-migration")
    public MigrationResult fullMigration(@RequestBody MigrationConfig config) {
        log.info("开始全量数据迁移: {}", config);
        return esMigrationService.fullMigration(config);
    }

    @ApiOperation("增量数据迁移")
    @PostMapping("/incremental-migration")
    public MigrationResult incrementalMigration(
            @RequestBody MigrationConfig config,
            @ApiParam("上次同步时间") @RequestParam(required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date lastSyncTime) {

        log.info("开始增量数据迁移: {}, 上次同步时间: {}", config, lastSyncTime);

        // 如果没有提供上次同步时间，尝试从目标索引获取
        if (lastSyncTime == null) {
            lastSyncTime = esMigrationService.getLastSyncTime(config.getTargetIndex(), config.getTimestampField());
        }

        return esMigrationService.incrementalMigration(config, lastSyncTime);
    }

    @ApiOperation("检查索引是否存在")
    @GetMapping("/index-exists")
    public Map<String, Object> indexExists(
            @ApiParam("索引名称") @RequestParam String indexName,
            @ApiParam("是否为目标ES") @RequestParam(defaultValue = "false") boolean isTarget) {

        boolean exists = esMigrationService.indexExists(indexName, isTarget);
        Map<String, Object> result = new HashMap<>();
        result.put("indexName", indexName);
        result.put("exists", exists);
        result.put("isTarget", isTarget);
        return result;
    }

    @ApiOperation("创建索引")
    @PostMapping("/create-index")
    public Map<String, Object> createIndex(
            @ApiParam("索引名称") @RequestParam String indexName,
            @ApiParam("索引映射JSON") @RequestParam(required = false) String mapping,
            @ApiParam("是否为目标ES") @RequestParam(defaultValue = "true") boolean isTarget) {

        boolean success = esMigrationService.createIndex(indexName, mapping, isTarget);
        Map<String, Object> result = new HashMap<>();
        result.put("indexName", indexName);
        result.put("success", success);
        result.put("isTarget", isTarget);
        return result;
    }

    @ApiOperation("获取索引映射")
    @GetMapping("/index-mapping")
    public Map<String, Object> getIndexMapping(
            @ApiParam("索引名称") @RequestParam String indexName,
            @ApiParam("是否为目标ES") @RequestParam(defaultValue = "false") boolean isTarget) {

        String mapping = esMigrationService.getIndexMapping(indexName, isTarget);
        Map<String, Object> result = new HashMap<>();
        result.put("indexName", indexName);
        result.put("mapping", mapping);
        result.put("isTarget", isTarget);
        return result;
    }

    @ApiOperation("获取文档数量")
    @GetMapping("/document-count")
    public Map<String, Object> getDocumentCount(
            @ApiParam("索引名称") @RequestParam String indexName,
            @ApiParam("是否为目标ES") @RequestParam(defaultValue = "false") boolean isTarget) {

        long count = esMigrationService.getDocumentCount(indexName, isTarget);
        Map<String, Object> result = new HashMap<>();
        result.put("indexName", indexName);
        result.put("count", count);
        result.put("isTarget", isTarget);
        result.put("note", "如果文档数量显示为10000，可能实际数量更多，请使用smart-document-count接口");
        return result;
    }

    @ApiOperation("智能获取索引文档数量（支持大数据量）")
    @GetMapping("/smart-document-count")
    public Map<String, Object> getSmartDocumentCount(
            @ApiParam("索引名称") @RequestParam String indexName,
            @ApiParam("是否为目标ES") @RequestParam(defaultValue = "false") boolean isTarget) {

        long count = ((EsMigrationServiceImpl) esMigrationService).getSmartDocumentCount(indexName, isTarget);
        Map<String, Object> result = new HashMap<>();
        result.put("indexName", indexName);
        result.put("count", count);
        result.put("isTarget", isTarget);
        result.put("method", "smart");
        result.put("note", "使用智能计数方法，支持大数据量索引的精确计数");
        return result;
    }

    @ApiOperation("获取大数据量索引的精确文档数量")
    @GetMapping("/large-document-count")
    public Map<String, Object> getLargeDocumentCount(
            @ApiParam("索引名称") @RequestParam String indexName,
            @ApiParam("是否为目标ES") @RequestParam(defaultValue = "false") boolean isTarget) {

        long count = ((EsMigrationServiceImpl) esMigrationService).getLargeIndexDocumentCount(indexName, isTarget);
        Map<String, Object> result = new HashMap<>();
        result.put("indexName", indexName);
        result.put("count", count);
        result.put("isTarget", isTarget);
        result.put("method", "large");
        result.put("note", "专门用于大数据量索引的精确计数，可能耗时较长");
        return result;
    }

    @ApiOperation("验证迁移结果")
    @PostMapping("/validate")
    public ValidationResult validateMigration(@RequestBody MigrationConfig config) {
        log.info("验证迁移结果: {}", config);
        return esMigrationService.validateMigration(config);
    }

    @ApiOperation("获取最后同步时间")
    @GetMapping("/last-sync-time")
    public Map<String, Object> getLastSyncTime(
            @ApiParam("索引名称") @RequestParam String indexName,
            @ApiParam("时间字段名") @RequestParam(defaultValue = "updateTime") String timestampField) {

        Date lastSyncTime = esMigrationService.getLastSyncTime(indexName, timestampField);
        Map<String, Object> result = new HashMap<>();
        result.put("indexName", indexName);
        result.put("timestampField", timestampField);
        result.put("lastSyncTime", lastSyncTime);
        return result;
    }

    @ApiOperation("获取默认迁移配置示例")
    @GetMapping("/default-config")
    public MigrationConfig getDefaultConfig() {
        MigrationConfig config = new MigrationConfig();
        config.setSourceIndex("source_index");
        config.setTargetIndex("target_index");
        config.setBatchSize(1000);
        config.setScrollTimeout(5);
        config.setIncrementalSync(false);
        config.setTimestampField("updateTime");
        config.setDeleteNotExist(false);
        config.setOverwriteExisting(true);
        config.setThreadCount(1);

        // 示例字段映射
        Map<String, String> fieldMapping = new HashMap<>();
        fieldMapping.put("old_field_name", "new_field_name");
        config.setFieldMapping(fieldMapping);

        // 示例新增字段
        Map<String, Object> newFields = new HashMap<>();
        newFields.put("migration_time", new Date());
        newFields.put("migration_version", "1.0");
        config.setNewFields(newFields);

        // 示例忽略大小写字段
        List<String> caseInsensitiveFields = Arrays.asList("title", "description");
        config.setCaseInsensitiveFields(caseInsensitiveFields);

        return config;
    }

    @ApiOperation("比较两个索引的差异")
    @GetMapping("/compare-indices")
    public Map<String, Object> compareIndices(
            @ApiParam("源索引名称") @RequestParam String sourceIndex,
            @ApiParam("目标索引名称") @RequestParam String targetIndex) {

        Map<String, Object> result = new HashMap<>();

        try {
            // 使用智能计数方法获取准确的文档数量
            long sourceCount = ((EsMigrationServiceImpl) esMigrationService).getSmartDocumentCount(sourceIndex, false);
            long targetCount = ((EsMigrationServiceImpl) esMigrationService).getSmartDocumentCount(targetIndex, true);

            result.put("sourceIndex", sourceIndex);
            result.put("targetIndex", targetIndex);
            result.put("sourceCount", sourceCount);
            result.put("targetCount", targetCount);
            result.put("difference", sourceCount - targetCount);
            result.put("isEqual", sourceCount == targetCount);

            if (sourceCount > 0) {
                result.put("completionRate", (double) targetCount / sourceCount * 100);
            } else {
                result.put("completionRate", 0.0);
            }

        } catch (Exception e) {
            log.error("比较索引失败", e);
            result.put("error", e.getMessage());
        }

        return result;
    }

}