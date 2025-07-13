package com.everflowx.esmigration.service;

import com.everflowx.esmigration.domain.IndexSyncConfig;
import com.everflowx.esmigration.domain.IndexSyncResult;

import java.util.List;

/**
 * 索引同步服务接口
 * 
 * @author everflowx
 */
public interface IndexSyncService {
    
    /**
     * 同步指定索引
     * 
     * @param config 同步配置
     * @return 同步结果
     */
    IndexSyncResult syncIndices(IndexSyncConfig config);
    
    /**
     * 同步单个索引
     * 
     * @param indexName 索引名称
     * @param config 同步配置
     * @return 同步结果
     */
    IndexSyncResult syncSingleIndex(String indexName, IndexSyncConfig config);
    
    /**
     * 获取所有索引列表
     * 
     * @param isTarget 是否为目标ES
     * @return 索引名称列表
     */
    List<String> getAllIndices(boolean isTarget);
    
    /**
     * 获取过滤后的索引列表
     * 
     * @param config 同步配置
     * @return 过滤后的索引名称列表
     */
    List<String> getFilteredIndices(IndexSyncConfig config);
    
    /**
     * 检查索引是否应该被同步
     * 
     * @param indexName 索引名称
     * @param config 同步配置
     * @return 是否应该同步
     */
    boolean shouldSyncIndex(String indexName, IndexSyncConfig config);
    
    /**
     * 创建索引（包括设置、映射、别名）
     * 
     * @param indexName 索引名称
     * @param config 同步配置
     * @return 是否创建成功
     */
    boolean createIndex(String indexName, IndexSyncConfig config);
    
    /**
     * 同步索引数据
     * 
     * @param indexName 索引名称
     * @param config 同步配置
     * @return 同步的文档数量
     */
    long syncIndexData(String indexName, IndexSyncConfig config);
    
    /**
     * 验证索引同步结果
     * 
     * @param indexName 索引名称
     * @return 验证结果
     */
    boolean validateIndexSync(String indexName);
    
    /**
     * 获取索引的设置信息
     * 
     * @param indexName 索引名称
     * @param isTarget 是否为目标ES
     * @return 设置信息JSON字符串
     */
    String getIndexSettings(String indexName, boolean isTarget);
    
    /**
     * 获取索引的映射信息
     * 
     * @param indexName 索引名称
     * @param isTarget 是否为目标ES
     * @return 映射信息JSON字符串
     */
    String getIndexMappings(String indexName, boolean isTarget);
    
    /**
     * 获取索引的别名信息
     * 
     * @param indexName 索引名称
     * @param isTarget 是否为目标ES
     * @return 别名信息
     */
    String getIndexAliases(String indexName, boolean isTarget);
}
