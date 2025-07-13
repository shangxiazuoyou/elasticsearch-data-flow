package com.everflowx.esmigration.domain;

import lombok.Data;

import java.util.List;

/**
 * 索引同步配置类
 * 
 * @author everflowx
 */
@Data
public class IndexSyncConfig {
    
    /**
     * 要同步的索引名称列表，如果为空则同步所有索引
     */
    private List<String> indexNames;
    
    /**
     * 是否覆盖已存在的索引
     */
    private Boolean overwriteExisting = false;
    
    /**
     * 是否同步索引设置（settings）
     */
    private Boolean syncSettings = true;
    
    /**
     * 是否同步索引映射（mappings）
     */
    private Boolean syncMappings = true;
    
    /**
     * 是否同步索引别名（aliases）
     */
    private Boolean syncAliases = true;
    
    /**
     * 是否同步数据
     */
    private Boolean syncData = true;
    
    /**
     * 批次大小
     */
    private Integer batchSize = 1000;
    
    /**
     * 滚动查询超时时间（分钟）
     */
    private Integer scrollTimeout = 10;
    
    /**
     * 线程数
     */
    private Integer threadCount = 2;
    
    /**
     * 排除的索引模式列表（支持通配符）
     */
    private List<String> excludePatterns;
    
    /**
     * 包含的索引模式列表（支持通配符）
     */
    private List<String> includePatterns;
    
    /**
     * 是否跳过系统索引（以.开头的索引）
     */
    private Boolean skipSystemIndices = true;
    
    /**
     * 是否在同步前验证连接
     */
    private Boolean validateConnection = true;
    
    /**
     * 是否在同步后验证数据
     */
    private Boolean validateData = true;
    
    /**
     * 新增字段配置（在同步时添加到每个文档）
     */
    private java.util.Map<String, Object> newFields;
    
    /**
     * 忽略大小写的字段列表
     */
    private List<String> caseInsensitiveFields;
    
    /**
     * 字段映射配置
     */
    private java.util.Map<String, String> fieldMapping;
    
    /**
     * 是否启用增量同步模式
     */
    private Boolean incrementalMode = false;
    
    /**
     * 增量同步的时间戳字段
     */
    private String timestampField = "updateTime";
    
    /**
     * 是否强制重新创建索引（删除后重建）
     */
    private Boolean forceRecreate = false;
    
    /**
     * 同步超时时间（分钟）
     */
    private Integer syncTimeoutMinutes = 60;
}
