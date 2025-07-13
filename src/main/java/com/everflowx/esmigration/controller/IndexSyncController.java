package com.everflowx.esmigration.controller;

import com.everflowx.esmigration.domain.IndexSyncConfig;
import com.everflowx.esmigration.domain.IndexSyncResult;
import com.everflowx.esmigration.service.IndexSyncService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 索引同步控制器
 * 
 * @author everflowx
 */
@Api(tags = "索引同步管理")
@RestController
@RequestMapping("/api/index-sync")
@Slf4j
public class IndexSyncController {
    
    @Autowired
    private IndexSyncService indexSyncService;
    
    @ApiOperation("同步指定索引或全部索引")
    @PostMapping("/sync")
    public IndexSyncResult syncIndices(@RequestBody IndexSyncConfig config) {
        log.info("开始索引同步，配置: {}", config);
        return indexSyncService.syncIndices(config);
    }
    
    @ApiOperation("同步单个索引")
    @PostMapping("/sync-single")
    public IndexSyncResult syncSingleIndex(
            @ApiParam("索引名称") @RequestParam String indexName,
            @RequestBody IndexSyncConfig config) {
        
        log.info("开始同步单个索引: {}", indexName);
        return indexSyncService.syncSingleIndex(indexName, config);
    }
    
    @ApiOperation("快速同步指定索引（使用默认配置）")
    @PostMapping("/quick-sync")
    public IndexSyncResult quickSync(
            @ApiParam("索引名称列表，用逗号分隔，不传则同步所有索引") 
            @RequestParam(required = false) String indexNames,
            @ApiParam("是否覆盖已存在的索引") 
            @RequestParam(defaultValue = "false") boolean overwrite,
            @ApiParam("批次大小") 
            @RequestParam(defaultValue = "1000") int batchSize,
            @ApiParam("线程数") 
            @RequestParam(defaultValue = "2") int threadCount) {
        
        IndexSyncConfig config = new IndexSyncConfig();
        
        if (indexNames != null && !indexNames.trim().isEmpty()) {
            config.setIndexNames(Arrays.asList(indexNames.split(",")));
        }
        
        config.setOverwriteExisting(overwrite);
        config.setBatchSize(batchSize);
        config.setThreadCount(threadCount);
        config.setSyncSettings(true);
        config.setSyncMappings(true);
        config.setSyncAliases(true);
        config.setSyncData(true);
        config.setSkipSystemIndices(true);
        config.setValidateConnection(true);
        config.setValidateData(true);
        
        log.info("快速同步索引: {}", indexNames != null ? indexNames : "全部索引");
        return indexSyncService.syncIndices(config);
    }
    
    @ApiOperation("仅创建索引结构（不同步数据）")
    @PostMapping("/create-structure")
    public IndexSyncResult createIndexStructure(
            @ApiParam("索引名称列表，用逗号分隔") @RequestParam String indexNames,
            @ApiParam("是否覆盖已存在的索引") @RequestParam(defaultValue = "false") boolean overwrite) {
        
        IndexSyncConfig config = new IndexSyncConfig();
        config.setIndexNames(Arrays.asList(indexNames.split(",")));
        config.setOverwriteExisting(overwrite);
        config.setSyncSettings(true);
        config.setSyncMappings(true);
        config.setSyncAliases(true);
        config.setSyncData(false); // 不同步数据
        config.setSkipSystemIndices(true);
        config.setValidateConnection(true);
        config.setValidateData(false);
        
        log.info("仅创建索引结构: {}", indexNames);
        return indexSyncService.syncIndices(config);
    }
    
    @ApiOperation("仅同步索引数据（索引必须已存在）")
    @PostMapping("/sync-data-only")
    public IndexSyncResult syncDataOnly(
            @ApiParam("索引名称列表，用逗号分隔") @RequestParam String indexNames,
            @ApiParam("批次大小") @RequestParam(defaultValue = "1000") int batchSize,
            @ApiParam("线程数") @RequestParam(defaultValue = "2") int threadCount,
            @ApiParam("是否增量同步") @RequestParam(defaultValue = "false") boolean incremental,
            @ApiParam("时间戳字段（增量同步时使用）") @RequestParam(defaultValue = "updateTime") String timestampField) {
        
        IndexSyncConfig config = new IndexSyncConfig();
        config.setIndexNames(Arrays.asList(indexNames.split(",")));
        config.setBatchSize(batchSize);
        config.setThreadCount(threadCount);
        config.setSyncSettings(false);
        config.setSyncMappings(false);
        config.setSyncAliases(false);
        config.setSyncData(true); // 只同步数据
        config.setIncrementalMode(incremental);
        config.setTimestampField(timestampField);
        config.setSkipSystemIndices(true);
        config.setValidateConnection(true);
        config.setValidateData(true);
        
        log.info("仅同步索引数据: {}, 增量模式: {}", indexNames, incremental);
        return indexSyncService.syncIndices(config);
    }
    
    @ApiOperation("获取所有索引列表")
    @GetMapping("/indices")
    public Map<String, Object> getAllIndices(
            @ApiParam("是否为目标ES") @RequestParam(defaultValue = "false") boolean isTarget) {
        
        List<String> indices = indexSyncService.getAllIndices(isTarget);
        
        Map<String, Object> result = new HashMap<>();
        result.put("isTarget", isTarget);
        result.put("totalCount", indices.size());
        result.put("indices", indices);
        
        return result;
    }
    
    @ApiOperation("获取过滤后的索引列表")
    @PostMapping("/filtered-indices")
    public Map<String, Object> getFilteredIndices(@RequestBody IndexSyncConfig config) {
        List<String> indices = indexSyncService.getFilteredIndices(config);
        
        Map<String, Object> result = new HashMap<>();
        result.put("totalCount", indices.size());
        result.put("indices", indices);
        result.put("config", config);
        
        return result;
    }
    
    @ApiOperation("检查索引是否应该被同步")
    @PostMapping("/should-sync")
    public Map<String, Object> shouldSyncIndex(
            @ApiParam("索引名称") @RequestParam String indexName,
            @RequestBody IndexSyncConfig config) {
        
        boolean shouldSync = indexSyncService.shouldSyncIndex(indexName, config);
        
        Map<String, Object> result = new HashMap<>();
        result.put("indexName", indexName);
        result.put("shouldSync", shouldSync);
        result.put("config", config);
        
        return result;
    }
    
    @ApiOperation("获取索引设置信息")
    @GetMapping("/index-settings")
    public Map<String, Object> getIndexSettings(
            @ApiParam("索引名称") @RequestParam String indexName,
            @ApiParam("是否为目标ES") @RequestParam(defaultValue = "false") boolean isTarget) {

        String settingsJson = indexSyncService.getIndexSettings(indexName, isTarget);

        Map<String, Object> result = new HashMap<>();
        result.put("indexName", indexName);
        result.put("isTarget", isTarget);

        // 将JSON字符串解析为对象，以便返回结构化的JSON
        try {
            Object settingsObj = com.alibaba.fastjson.JSON.parse(settingsJson);
            result.put("settings", settingsObj);
        } catch (Exception e) {
            // 如果解析失败，返回原始字符串
            result.put("settings", settingsJson);
            result.put("note", "设置信息解析失败，返回原始格式");
        }

        return result;
    }
    
    @ApiOperation("获取索引映射信息")
    @GetMapping("/index-mappings")
    public Map<String, Object> getIndexMappings(
            @ApiParam("索引名称") @RequestParam String indexName,
            @ApiParam("是否为目标ES") @RequestParam(defaultValue = "false") boolean isTarget) {

        String mappingsJson = indexSyncService.getIndexMappings(indexName, isTarget);

        Map<String, Object> result = new HashMap<>();
        result.put("indexName", indexName);
        result.put("isTarget", isTarget);

        // 将JSON字符串解析为对象，以便返回结构化的JSON
        try {
            Object mappingsObj = com.alibaba.fastjson.JSON.parse(mappingsJson);
            result.put("mappings", mappingsObj);
        } catch (Exception e) {
            // 如果解析失败，返回原始字符串
            result.put("mappings", mappingsJson);
            result.put("note", "映射信息解析失败，返回原始格式");
        }

        return result;
    }
    
    @ApiOperation("获取索引别名信息")
    @GetMapping("/index-aliases")
    public Map<String, Object> getIndexAliases(
            @ApiParam("索引名称") @RequestParam String indexName,
            @ApiParam("是否为目标ES") @RequestParam(defaultValue = "false") boolean isTarget) {

        String aliasesJson = indexSyncService.getIndexAliases(indexName, isTarget);

        Map<String, Object> result = new HashMap<>();
        result.put("indexName", indexName);
        result.put("isTarget", isTarget);

        // 将JSON字符串解析为对象，以便返回结构化的JSON
        try {
            Object aliasesObj = com.alibaba.fastjson.JSON.parse(aliasesJson);
            result.put("aliases", aliasesObj);
        } catch (Exception e) {
            // 如果解析失败，返回原始字符串
            result.put("aliases", aliasesJson);
            result.put("note", "别名信息解析失败，返回原始格式");
        }

        return result;
    }
    
    @ApiOperation("验证索引同步结果")
    @GetMapping("/validate")
    public Map<String, Object> validateIndexSync(
            @ApiParam("索引名称") @RequestParam String indexName) {
        
        boolean isValid = indexSyncService.validateIndexSync(indexName);
        
        Map<String, Object> result = new HashMap<>();
        result.put("indexName", indexName);
        result.put("isValid", isValid);
        result.put("message", isValid ? "验证成功" : "验证失败，请检查文档数量是否一致");
        
        return result;
    }

    @ApiOperation("获取索引完整信息（包括设置、映射、别名）")
    @GetMapping("/index-info")
    public Map<String, Object> getIndexInfo(
            @ApiParam("索引名称") @RequestParam String indexName,
            @ApiParam("是否为目标ES") @RequestParam(defaultValue = "false") boolean isTarget) {

        Map<String, Object> result = new HashMap<>();
        result.put("indexName", indexName);
        result.put("isTarget", isTarget);

        // 获取设置信息
        try {
            String settingsJson = indexSyncService.getIndexSettings(indexName, isTarget);
            Object settingsObj = com.alibaba.fastjson.JSON.parse(settingsJson);
            result.put("settings", settingsObj);
        } catch (Exception e) {
            result.put("settings", "{}");
            result.put("settingsError", "获取设置信息失败: " + e.getMessage());
        }

        // 获取映射信息
        try {
            String mappingsJson = indexSyncService.getIndexMappings(indexName, isTarget);
            Object mappingsObj = com.alibaba.fastjson.JSON.parse(mappingsJson);
            result.put("mappings", mappingsObj);
        } catch (Exception e) {
            result.put("mappings", "{}");
            result.put("mappingsError", "获取映射信息失败: " + e.getMessage());
        }

        // 获取别名信息
        try {
            String aliasesJson = indexSyncService.getIndexAliases(indexName, isTarget);
            Object aliasesObj = com.alibaba.fastjson.JSON.parse(aliasesJson);
            result.put("aliases", aliasesObj);
        } catch (Exception e) {
            result.put("aliases", "{}");
            result.put("aliasesError", "获取别名信息失败: " + e.getMessage());
        }

        return result;
    }

    @ApiOperation("比较两个ES中同名索引的配置差异")
    @GetMapping("/compare-index-config")
    public Map<String, Object> compareIndexConfig(
            @ApiParam("索引名称") @RequestParam String indexName) {

        Map<String, Object> result = new HashMap<>();
        result.put("indexName", indexName);

        try {
            // 获取源ES索引信息
            Map<String, Object> sourceInfo = new HashMap<>();
            sourceInfo.put("settings", com.alibaba.fastjson.JSON.parse(indexSyncService.getIndexSettings(indexName, false)));
            sourceInfo.put("mappings", com.alibaba.fastjson.JSON.parse(indexSyncService.getIndexMappings(indexName, false)));
            sourceInfo.put("aliases", com.alibaba.fastjson.JSON.parse(indexSyncService.getIndexAliases(indexName, false)));

            // 获取目标ES索引信息
            Map<String, Object> targetInfo = new HashMap<>();
            targetInfo.put("settings", com.alibaba.fastjson.JSON.parse(indexSyncService.getIndexSettings(indexName, true)));
            targetInfo.put("mappings", com.alibaba.fastjson.JSON.parse(indexSyncService.getIndexMappings(indexName, true)));
            targetInfo.put("aliases", com.alibaba.fastjson.JSON.parse(indexSyncService.getIndexAliases(indexName, true)));

            result.put("source", sourceInfo);
            result.put("target", targetInfo);

            // 简单的差异检查
            boolean settingsMatch = sourceInfo.get("settings").toString().equals(targetInfo.get("settings").toString());
            boolean mappingsMatch = sourceInfo.get("mappings").toString().equals(targetInfo.get("mappings").toString());
            boolean aliasesMatch = sourceInfo.get("aliases").toString().equals(targetInfo.get("aliases").toString());

            result.put("configMatch", settingsMatch && mappingsMatch && aliasesMatch);
            result.put("settingsMatch", settingsMatch);
            result.put("mappingsMatch", mappingsMatch);
            result.put("aliasesMatch", aliasesMatch);

        } catch (Exception e) {
            result.put("error", "比较索引配置失败: " + e.getMessage());
            result.put("configMatch", false);
        }

        return result;
    }
}
