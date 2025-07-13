package com.everflowx.esmigration.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;
import java.util.Map;

/**
 * 全局异常处理器
 * 
 * @author everflowx
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {
    
    @ExceptionHandler(EsMigrationException.class)
    public ResponseEntity<Map<String, Object>> handleEsMigrationException(EsMigrationException e) {
        log.error("ES迁移异常: {}", e.toString(), e);
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("errorCode", e.getErrorCode());
        response.put("message", e.getMessage());
        response.put("indexName", e.getIndexName());
        response.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    @ExceptionHandler(IndexNotExistsException.class)
    public ResponseEntity<Map<String, Object>> handleIndexNotExistsException(IndexNotExistsException e) {
        log.error("索引不存在异常: {}", e.toString());
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("errorCode", e.getErrorCode());
        response.put("message", e.getMessage());
        response.put("indexName", e.getIndexName());
        response.put("suggestion", "请检查索引名称是否正确，或先创建该索引");
        response.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
    }
    
    @ExceptionHandler(MigrationConfigException.class)
    public ResponseEntity<Map<String, Object>> handleMigrationConfigException(MigrationConfigException e) {
        log.error("配置异常: {}", e.getMessage());
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("errorCode", e.getErrorCode());
        response.put("message", e.getMessage());
        response.put("suggestion", "请检查配置参数是否正确");
        response.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    @ExceptionHandler(EsConnectionException.class)
    public ResponseEntity<Map<String, Object>> handleEsConnectionException(EsConnectionException e) {
        log.error("ES连接异常: {}", e.toString(), e);
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("errorCode", e.getErrorCode());
        response.put("message", e.getMessage());
        response.put("esHost", e.getEsHost());
        response.put("esPort", e.getEsPort());
        response.put("esType", e.getEsType());
        response.put("suggestion", "请检查ES服务是否正常运行，网络连接是否正常，认证信息是否正确");
        response.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
    }
    
    @ExceptionHandler(DataMigrationException.class)
    public ResponseEntity<Map<String, Object>> handleDataMigrationException(DataMigrationException e) {
        log.error("数据迁移异常: {}", e.toString(), e);
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("errorCode", e.getErrorCode());
        response.put("message", e.getMessage());
        response.put("indexName", e.getIndexName());
        
        if (e.getProcessedCount() > 0) {
            response.put("processedCount", e.getProcessedCount());
        }
        if (e.getFailedCount() > 0) {
            response.put("failedCount", e.getFailedCount());
        }
        if (e.getBatchId() != null) {
            response.put("batchId", e.getBatchId());
        }
        
        response.put("suggestion", "请检查源数据和目标ES的状态，考虑减小批次大小或重试");
        response.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
    
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgumentException(IllegalArgumentException e) {
        log.error("参数异常: {}", e.getMessage());
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("errorCode", "INVALID_PARAMETER");
        response.put("message", "参数错误: " + e.getMessage());
        response.put("suggestion", "请检查请求参数是否正确");
        response.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Map<String, Object>> handleRuntimeException(RuntimeException e) {
        log.error("运行时异常: {}", e.getMessage(), e);
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("errorCode", "RUNTIME_ERROR");
        response.put("message", "系统内部错误: " + e.getMessage());
        response.put("suggestion", "请联系管理员或稍后重试");
        response.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
    
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneralException(Exception e) {
        log.error("未知异常: {}", e.getMessage(), e);
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("errorCode", "UNKNOWN_ERROR");
        response.put("message", "系统发生未知错误: " + e.getMessage());
        response.put("suggestion", "请联系管理员");
        response.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}