package com.everflowx.esmigration.exception;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * 异常处理工具类
 * 提供统一的异常转换、重试机制和错误恢复策略
 * 
 * @author everflowx
 */
@Slf4j
public class ExceptionHandler {
    
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_RETRY_DELAY_MS = 1000;
    
    /**
     * 执行带有重试机制的操作
     */
    public static <T> T executeWithRetry(Callable<T> operation, String operationName) {
        return executeWithRetry(operation, operationName, DEFAULT_MAX_RETRIES);
    }
    
    /**
     * 执行带有重试机制的操作
     */
    public static <T> T executeWithRetry(Callable<T> operation, String operationName, int maxRetries) {
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= maxRetries + 1; attempt++) {
            try {
                return operation.call();
            } catch (ConnectException | SocketTimeoutException e) {
                lastException = e;
                if (attempt <= maxRetries) {
                    long delay = DEFAULT_RETRY_DELAY_MS * attempt; // 指数退避
                    log.warn("{}失败，第{}次重试，{}ms后重试: {}", operationName, attempt, delay, e.getMessage());
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new EsMigrationException("OPERATION_INTERRUPTED", 
                            operationName + "被中断", ie);
                    }
                } else {
                    log.error("{}重试{}次后仍然失败", operationName, maxRetries);
                }
            } catch (Exception e) {
                // 非网络异常，直接抛出
                throw convertToBusinessException(e, operationName);
            }
        }
        
        // 重试次数用尽，抛出网络异常
        throw new EsConnectionException("NETWORK_RETRY_EXHAUSTED", 
            String.format("%s重试%d次后仍然失败: %s", operationName, maxRetries, lastException.getMessage()),
            lastException);
    }
    
    /**
     * 将通用异常转换为业务异常
     */
    public static RuntimeException convertToBusinessException(Exception e, String context) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        
        if (e instanceof ElasticsearchException) {
            ElasticsearchException esException = (ElasticsearchException) e;
            return new EsConnectionException("ES_ERROR", 
                context + "时ES异常: " + esException.getDetailedMessage(), e);
        }
        
        if (e instanceof IOException) {
            return new EsConnectionException("ES_IO_ERROR", 
                context + "时IO异常: " + e.getMessage(), e);
        }
        
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return new EsMigrationException("OPERATION_INTERRUPTED", 
                context + "被中断: " + e.getMessage(), e);
        }
        
        if (e instanceof IllegalArgumentException) {
            return MigrationConfigException.withCustomErrorCode("INVALID_PARAMETER", 
                context + "时参数错误: " + e.getMessage());
        }
        
        // 未知异常
        return new EsMigrationException("UNKNOWN_ERROR", 
            context + "时发生未知异常: " + e.getMessage(), e);
    }
    
    /**
     * 安全执行操作，自动转换异常
     */
    public static <T> T safeExecute(Callable<T> operation, String operationName) {
        try {
            return operation.call();
        } catch (Exception e) {
            throw convertToBusinessException(e, operationName);
        }
    }
    
    /**
     * 安全执行操作，返回Optional结果
     */
    public static <T> T safeExecuteWithDefault(Callable<T> operation, T defaultValue, String operationName) {
        try {
            return operation.call();
        } catch (Exception e) {
            log.error("{}失败，返回默认值: {}", operationName, e.getMessage(), e);
            return defaultValue;
        }
    }
    
    /**
     * 处理批量操作失败
     */
    public static void handleBulkFailures(BulkResponse bulkResponse, String indexName, 
                                         Function<String, Void> failureHandler) {
        if (!bulkResponse.hasFailures()) {
            return;
        }
        
        int totalFailures = 0;
        StringBuilder errorSummary = new StringBuilder();
        
        for (BulkItemResponse item : bulkResponse.getItems()) {
            if (item.isFailed()) {
                totalFailures++;
                BulkItemResponse.Failure failure = item.getFailure();
                String errorMsg = String.format("文档ID: %s, 错误: %s", 
                    item.getId(), failure.getMessage());
                errorSummary.append(errorMsg).append("; ");
                
                // 调用失败处理器
                if (failureHandler != null) {
                    failureHandler.apply(item.getId());
                }
            }
        }
        
        if (totalFailures > 0) {
            log.error("批量操作失败，索引: {}, 失败数量: {}, 错误详情: {}", 
                indexName, totalFailures, errorSummary.toString());
            
            // 如果失败率过高，抛出异常
            double failureRate = (double) totalFailures / bulkResponse.getItems().length;
            if (failureRate > 0.5) { // 失败率超过50%
                throw DataMigrationException.bulkOperationFailed(indexName, 
                    "BULK_" + System.currentTimeMillis(), totalFailures, 
                    new RuntimeException(errorSummary.toString()));
            }
        }
    }
    
    /**
     * 线程安全的中断检查
     */
    public static void checkInterrupted(String operationName) {
        if (Thread.currentThread().isInterrupted()) {
            throw new EsMigrationException("OPERATION_INTERRUPTED", 
                operationName + "被中断");
        }
    }
    
    /**
     * 记录和包装异常信息
     */
    public static void logAndThrow(Exception e, String errorCode, String message, Object... args) {
        String formattedMessage = String.format(message, args);
        log.error(formattedMessage, e);
        throw new EsMigrationException(errorCode, formattedMessage, e);
    }
    
    /**
     * 断路器模式：在连续失败次数达到阈值时停止操作
     */
    public static class CircuitBreaker {
        private final int failureThreshold;
        private int consecutiveFailures = 0;
        private boolean circuitOpen = false;
        
        public CircuitBreaker(int failureThreshold) {
            this.failureThreshold = failureThreshold;
        }
        
        public <T> T execute(Callable<T> operation, String operationName) {
            if (circuitOpen) {
                throw new EsMigrationException("CIRCUIT_BREAKER_OPEN", 
                    "断路器已打开，" + operationName + "暂时不可用");
            }
            
            try {
                T result = operation.call();
                consecutiveFailures = 0; // 重置失败计数
                return result;
            } catch (Exception e) {
                consecutiveFailures++;
                if (consecutiveFailures >= failureThreshold) {
                    circuitOpen = true;
                    log.error("连续失败{}次，断路器打开，停止{}", consecutiveFailures, operationName);
                }
                throw convertToBusinessException(e, operationName);
            }
        }
        
        public void reset() {
            consecutiveFailures = 0;
            circuitOpen = false;
        }
        
        public boolean isOpen() {
            return circuitOpen;
        }
    }
}