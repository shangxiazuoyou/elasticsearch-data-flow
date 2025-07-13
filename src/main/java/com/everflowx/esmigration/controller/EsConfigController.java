package com.everflowx.esmigration.controller;

import com.everflowx.esmigration.config.ElasticsearchConfig;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * ES配置管理控制器
 * 支持动态调整ES客户端配置
 *
 * @author everflowx
 */
@Api(tags = "ES配置管理")
@RestController
@RequestMapping("/api/es-config")
@Slf4j
public class EsConfigController {

    @Autowired
    private ElasticsearchConfig elasticsearchConfig;

    @ApiOperation("获取当前响应缓冲区配置")
    @GetMapping("/buffer-config")
    public Map<String, Object> getBufferConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("bufferLimit", elasticsearchConfig.getResponseBufferLimit());
        config.put("bufferLimitMB", elasticsearchConfig.getResponseBufferLimit() / 1024 / 1024);
        config.put("bufferInitial", elasticsearchConfig.getResponseBufferInitial());
        config.put("bufferInitialKB", elasticsearchConfig.getResponseBufferInitial() / 1024);
        return config;
    }

    @ApiOperation("动态调整响应缓冲区大小")
    @PostMapping("/update-buffer-limit")
    public Map<String, Object> updateBufferLimit(
            @ApiParam("新的缓冲区大小（MB）") @RequestParam int limitMB) {

        int newLimitBytes = limitMB * 1024 * 1024;
        int oldLimitBytes = elasticsearchConfig.getResponseBufferLimit();

        elasticsearchConfig.updateResponseBufferLimit(newLimitBytes);

        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("oldLimitMB", oldLimitBytes / 1024 / 1024);
        result.put("newLimitMB", limitMB);
        result.put("message", String.format("响应缓冲区大小已从 %dMB 调整为 %dMB",
                oldLimitBytes / 1024 / 1024, limitMB));

        log.info("动态调整响应缓冲区大小: {}MB -> {}MB", oldLimitBytes / 1024 / 1024, limitMB);

        return result;
    }

    @ApiOperation("获取推荐的缓冲区配置")
    @GetMapping("/recommended-buffer")
    public Map<String, Object> getRecommendedBuffer(
            @ApiParam("预期的数据量级别：small/medium/large/xlarge")
            @RequestParam(defaultValue = "medium") String dataSize) {

        Map<String, Object> recommendation = new HashMap<>();

        switch (dataSize.toLowerCase()) {
            case "small":
                // 小数据量：< 1万条记录
                recommendation.put("bufferLimitMB", 50);
                recommendation.put("batchSize", 500);
                recommendation.put("threadCount", 1);
                recommendation.put("description", "适用于小数据量迁移（< 1万条记录）");
                break;
            case "medium":
                // 中等数据量：1万 - 100万条记录
                recommendation.put("bufferLimitMB", 100);
                recommendation.put("batchSize", 1000);
                recommendation.put("threadCount", 2);
                recommendation.put("description", "适用于中等数据量迁移（1万 - 100万条记录）");
                break;
            case "large":
                // 大数据量：100万 - 1000万条记录
                recommendation.put("bufferLimitMB", 200);
                recommendation.put("batchSize", 2000);
                recommendation.put("threadCount", 3);
                recommendation.put("description", "适用于大数据量迁移（100万 - 1000万条记录）");
                break;
            case "xlarge":
                // 超大数据量：> 1000万条记录
                recommendation.put("bufferLimitMB", 500);
                recommendation.put("batchSize", 5000);
                recommendation.put("threadCount", 4);
                recommendation.put("description", "适用于超大数据量迁移（> 1000万条记录）");
                break;
            default:
                recommendation.put("bufferLimitMB", 100);
                recommendation.put("batchSize", 1000);
                recommendation.put("threadCount", 2);
                recommendation.put("description", "默认配置");
        }

        recommendation.put("dataSize", dataSize);
        recommendation.put("note", "建议根据实际内存情况和ES集群性能进行调整");

        return recommendation;
    }

    @ApiOperation("内存使用情况监控")
    @GetMapping("/memory-status")
    public Map<String, Object> getMemoryStatus() {
        Runtime runtime = Runtime.getRuntime();

        Map<String, Object> memoryInfo = new HashMap<>();
        memoryInfo.put("totalMemoryMB", runtime.totalMemory() / 1024 / 1024);
        memoryInfo.put("freeMemoryMB", runtime.freeMemory() / 1024 / 1024);
        memoryInfo.put("usedMemoryMB", (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024);
        memoryInfo.put("maxMemoryMB", runtime.maxMemory() / 1024 / 1024);

        // 计算内存使用率
        double usagePercent = ((double)(runtime.totalMemory() - runtime.freeMemory()) / runtime.maxMemory()) * 100;
        memoryInfo.put("usagePercent", Math.round(usagePercent * 100.0) / 100.0);

        // 当前缓冲区配置
        memoryInfo.put("currentBufferLimitMB", elasticsearchConfig.getResponseBufferLimit() / 1024 / 1024);

        // 内存状态评估
        String status;
        String recommendation;
        if (usagePercent < 50) {
            status = "良好";
            recommendation = "内存使用正常，可以考虑增加缓冲区大小以提升性能";
        } else if (usagePercent < 80) {
            status = "中等";
            recommendation = "内存使用适中，建议保持当前配置";
        } else {
            status = "紧张";
            recommendation = "内存使用较高，建议减少缓冲区大小或增加JVM内存";
        }

        memoryInfo.put("status", status);
        memoryInfo.put("recommendation", recommendation);

        return memoryInfo;
    }

    @ApiOperation("性能调优建议")
    @GetMapping("/performance-advice")
    public Map<String, Object> getPerformanceAdvice(
            @ApiParam("索引文档数量") @RequestParam(required = false) Long documentCount,
            @ApiParam("平均文档大小（KB）") @RequestParam(required = false) Integer avgDocSizeKB) {

        Map<String, Object> advice = new HashMap<>();

        // 基于文档数量的建议
        if (documentCount != null) {
            if (documentCount < 10000) {
                advice.put("bufferSizeMB", 50);
                advice.put("batchSize", 500);
                advice.put("threadCount", 1);
            } else if (documentCount < 1000000) {
                advice.put("bufferSizeMB", 100);
                advice.put("batchSize", 1000);
                advice.put("threadCount", 2);
            } else if (documentCount < 10000000) {
                advice.put("bufferSizeMB", 200);
                advice.put("batchSize", 2000);
                advice.put("threadCount", 3);
            } else {
                advice.put("bufferSizeMB", 500);
                advice.put("batchSize", 5000);
                advice.put("threadCount", 4);
            }
        }

        // 基于文档大小的调整
        if (avgDocSizeKB != null) {
            int currentBufferMB = (Integer) advice.getOrDefault("bufferSizeMB", 100);
            int currentBatchSize = (Integer) advice.getOrDefault("batchSize", 1000);

            if (avgDocSizeKB > 100) { // 大文档
                advice.put("bufferSizeMB", Math.max(currentBufferMB * 2, 200));
                advice.put("batchSize", Math.max(currentBatchSize / 2, 100));
                advice.put("note", "检测到大文档，已调整缓冲区和批次大小");
            } else if (avgDocSizeKB < 10) { // 小文档
                advice.put("batchSize", Math.min(currentBatchSize * 2, 10000));
                advice.put("note", "检测到小文档，可以增加批次大小");
            }
        }

        advice.put("generalTips", java.util.Arrays.asList(
            "监控ES集群的CPU和内存使用情况",
            "根据网络带宽调整批次大小",
            "在业务低峰期执行大数据量迁移",
            "定期检查迁移日志和错误信息"
        ));

        return advice;
    }
}
