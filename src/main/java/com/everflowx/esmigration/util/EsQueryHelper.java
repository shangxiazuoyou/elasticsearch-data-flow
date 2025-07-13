package com.everflowx.esmigration.util;

import com.everflowx.esmigration.config.ElasticsearchConfig;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * ES查询辅助工具类
 * 提供带重试机制和错误处理的ES查询方法
 * 
 * @author everflowx
 */
@Slf4j
@Component
public class EsQueryHelper {
    
    @Autowired
    private ElasticsearchConfig elasticsearchConfig;
    
    /**
     * 执行带重试机制的搜索请求
     */
    public SearchResponse searchWithRetry(RestHighLevelClient client, SearchRequest searchRequest, 
                                        boolean useLargeBuffer, int maxRetries) throws IOException {
        
        int retryCount = 0;
        Exception lastException = null;
        
        while (retryCount < maxRetries) {
            try {
                RequestOptions options = useLargeBuffer ? 
                    elasticsearchConfig.getLargeQueryRequestOptions() : 
                    elasticsearchConfig.getCustomRequestOptions();
                
                log.debug("执行搜索请求，重试次数: {}, 使用大缓冲区: {}", retryCount, useLargeBuffer);
                return client.search(searchRequest, options);
                
            } catch (Exception e) {
                lastException = e;
                retryCount++;
                
                log.warn("搜索请求失败，重试次数: {}/{}, 错误: {}", retryCount, maxRetries, e.getMessage());
                
                if (retryCount >= maxRetries) {
                    break;
                }
                
                // 如果是解析错误，尝试减少批次大小
                if (e.getMessage().contains("Unable to parse response body") ||
                    e.getMessage().contains("Content length")) {

                    int currentSize = searchRequest.source().size();
                    int newSize = Math.max(currentSize / 2, 10); // 最小10条
                    searchRequest.source().size(newSize);
                    // 确保trackTotalHits设置保持不变
                    searchRequest.source().trackTotalHits(true);

                    log.info("检测到响应解析错误，减少批次大小: {} -> {}", currentSize, newSize);
                }
                
                // 递增延迟
                try {
                    Thread.sleep(1000 * retryCount);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("重试被中断", ie);
                }
            }
        }
        
        throw new IOException("搜索请求重试失败: " + lastException.getMessage(), lastException);
    }
    
    /**
     * 执行带重试机制的滚动请求
     */
    public SearchResponse scrollWithRetry(RestHighLevelClient client, SearchScrollRequest scrollRequest, 
                                        boolean useLargeBuffer, int maxRetries) throws IOException {
        
        int retryCount = 0;
        Exception lastException = null;
        
        while (retryCount < maxRetries) {
            try {
                RequestOptions options = useLargeBuffer ? 
                    elasticsearchConfig.getLargeQueryRequestOptions() : 
                    elasticsearchConfig.getCustomRequestOptions();
                
                log.debug("执行滚动请求，重试次数: {}, 使用大缓冲区: {}", retryCount, useLargeBuffer);
                return client.scroll(scrollRequest, options);
                
            } catch (Exception e) {
                lastException = e;
                retryCount++;
                
                log.warn("滚动请求失败，重试次数: {}/{}, 错误: {}", retryCount, maxRetries, e.getMessage());
                
                if (retryCount >= maxRetries) {
                    break;
                }
                
                // 递增延迟
                try {
                    Thread.sleep(1000 * retryCount);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("重试被中断", ie);
                }
            }
        }
        
        throw new IOException("滚动请求重试失败: " + lastException.getMessage(), lastException);
    }
    
    /**
     * 检查异常是否为响应解析错误
     */
    public boolean isResponseParseError(Exception e) {
        String message = e.getMessage();
        return message != null && (
            message.contains("Unable to parse response body") ||
            message.contains("Content length") ||
            message.contains("Response body") ||
            message.contains("JSON parse error")
        );
    }
    
    /**
     * 根据错误类型建议批次大小调整
     */
    public int suggestBatchSizeAdjustment(Exception e, int currentBatchSize) {
        if (isResponseParseError(e)) {
            // 响应解析错误，大幅减少批次大小
            return Math.max(currentBatchSize / 4, 10);
        } else if (e.getMessage().contains("timeout")) {
            // 超时错误，适度减少批次大小
            return Math.max(currentBatchSize / 2, 50);
        } else {
            // 其他错误，小幅减少批次大小
            return Math.max((int)(currentBatchSize * 0.8), 100);
        }
    }
    
    /**
     * 获取当前缓冲区配置信息
     */
    public String getBufferConfigInfo() {
        int bufferMB = elasticsearchConfig.getResponseBufferLimit() / 1024 / 1024;
        return String.format("当前响应缓冲区: %dMB", bufferMB);
    }
}
