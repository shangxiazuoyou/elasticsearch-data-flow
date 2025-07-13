package com.everflowx.esmigration.domain;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * 迁移配置实体类
 * 
 * @author everflowx
 */
@Data
public class MigrationConfig {
    
    /**
     * 源索引名称
     */
    private String sourceIndex;
    
    /**
     * 目标索引名称
     */
    private String targetIndex;
    
    /**
     * 批次大小
     */
    private Integer batchSize = 1000;
    
    /**
     * 滚动查询超时时间（分钟）
     */
    private Integer scrollTimeout = 5;
    
    /**
     * 字段映射配置
     * key: 源字段名, value: 目标字段名
     */
    private Map<String, String> fieldMapping;
    
    /**
     * 新增字段配置
     * key: 字段名, value: 默认值
     */
    private Map<String, Object> newFields;
    
    /**
     * 忽略大小写的字段列表
     */
    private List<String> caseInsensitiveFields;
    
    /**
     * 是否启用增量同步
     */
    private Boolean incrementalSync = false;
    
    /**
     * 增量同步时间字段名
     */
    private String timestampField = "updateTime";
    
    /**
     * 是否删除目标索引中不存在于源索引的文档
     */
    private Boolean deleteNotExist = false;
    
    /**
     * 查询条件（JSON格式）
     */
    private String queryCondition;
    
    /**
     * 排除字段列表
     */
    private List<String> excludeFields;
    
    /**
     * 包含字段列表（如果设置，则只同步这些字段）
     */
    private List<String> includeFields;
    
    /**
     * 是否覆盖已存在的文档
     */
    private Boolean overwriteExisting = true;
    
    /**
     * 线程数
     */
    private Integer threadCount = 1;
}
