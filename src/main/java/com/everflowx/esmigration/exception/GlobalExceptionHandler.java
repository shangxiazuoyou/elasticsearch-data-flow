package com.everflowx.esmigration.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindException;
import org.springframework.validation.FieldError;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 全局异常处理器
 * 增强版本，包含详细的异常分类处理、统计监控和建议反馈
 * 
 * @author everflowx
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {
    
    // 异常统计计数器
    private final AtomicLong totalExceptions = new AtomicLong(0);
    private final AtomicLong businessExceptions = new AtomicLong(0);
    private final AtomicLong systemExceptions = new AtomicLong(0);
    private final AtomicLong networkExceptions = new AtomicLong(0);
    private final AtomicLong validationExceptions = new AtomicLong(0);
    
    /**
     * 获取异常统计信息
     */
    public Map<String, Object> getExceptionStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalExceptions", totalExceptions.get());
        stats.put("businessExceptions", businessExceptions.get());
        stats.put("systemExceptions", systemExceptions.get());
        stats.put("networkExceptions", networkExceptions.get());
        stats.put("validationExceptions", validationExceptions.get());
        return stats;
    }
    
    /**
     * 创建统一的错误响应格式
     */
    private Map<String, Object> createErrorResponse(String errorCode, String message, 
                                                   String suggestion, HttpServletRequest request) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("errorCode", errorCode);
        response.put("message", message);
        response.put("suggestion", suggestion);
        response.put("timestamp", System.currentTimeMillis());
        response.put("path", request != null ? request.getRequestURI() : "unknown");
        
        // 生成错误追踪ID
        String traceId = String.format("ERR-%d-%d", 
            System.currentTimeMillis() % 100000, 
            Thread.currentThread().getId());
        response.put("traceId", traceId);
        
        return response;
    }
    
    @ExceptionHandler(EsMigrationException.class)
    public ResponseEntity<Map<String, Object>> handleEsMigrationException(EsMigrationException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        businessExceptions.incrementAndGet();
        
        log.error("ES迁移异常 [{}]: {}", e.getErrorCode(), e.toString(), e);
        
        Map<String, Object> response = createErrorResponse(
            e.getErrorCode(), 
            e.getMessage(), 
            "请检查ES集群状态和迁移配置，或联系技术支持", 
            request
        );
        response.put("indexName", e.getIndexName());
        response.put("exceptionType", "BUSINESS_EXCEPTION");
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    @ExceptionHandler(IndexNotExistsException.class)
    public ResponseEntity<Map<String, Object>> handleIndexNotExistsException(IndexNotExistsException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        businessExceptions.incrementAndGet();
        
        log.error("索引不存在异常 [{}]: {}", e.getErrorCode(), e.toString());
        
        Map<String, Object> response = createErrorResponse(
            e.getErrorCode(),
            e.getMessage(),
            String.format("索引 '%s' 不存在，请检查索引名称是否正确，或先创建该索引", e.getIndexName()),
            request
        );
        response.put("indexName", e.getIndexName());
        response.put("exceptionType", "RESOURCE_NOT_FOUND");
        
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
    }
    
    @ExceptionHandler(MigrationConfigException.class)
    public ResponseEntity<Map<String, Object>> handleMigrationConfigException(MigrationConfigException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        validationExceptions.incrementAndGet();
        
        log.error("配置异常 [{}]: {}", e.getErrorCode(), e.getMessage());
        
        Map<String, Object> response = createErrorResponse(
            e.getErrorCode(),
            e.getMessage(),
            "请检查配置参数格式、数值范围和必填项是否正确",
            request
        );
        response.put("exceptionType", "VALIDATION_ERROR");
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    @ExceptionHandler(EsConnectionException.class)
    public ResponseEntity<Map<String, Object>> handleEsConnectionException(EsConnectionException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        networkExceptions.incrementAndGet();
        
        log.error("ES连接异常 [{}]: {} - {}:{}", e.getErrorCode(), e.toString(), e.getEsHost(), e.getEsPort(), e);
        
        Map<String, Object> response = createErrorResponse(
            e.getErrorCode(),
            e.getMessage(),
            String.format("无法连接到%s ES服务器 %s:%d，请检查服务状态、网络连接和认证信息", 
                e.getEsType(), e.getEsHost(), e.getEsPort()),
            request
        );
        response.put("esHost", e.getEsHost());
        response.put("esPort", e.getEsPort());
        response.put("esType", e.getEsType());
        response.put("exceptionType", "NETWORK_ERROR");
        
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
    }
    
    @ExceptionHandler(DataMigrationException.class)
    public ResponseEntity<Map<String, Object>> handleDataMigrationException(DataMigrationException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        businessExceptions.incrementAndGet();
        
        log.error("数据迁移异常 [{}]: {} - 处理数量:{}, 失败数量:{}", 
            e.getErrorCode(), e.toString(), e.getProcessedCount(), e.getFailedCount(), e);
        
        String suggestion = String.format("迁移失败，已处理%d条记录，失败%d条。建议：1)检查源数据质量 2)减小批次大小至%d 3)检查目标ES磁盘空间",
            e.getProcessedCount(), e.getFailedCount(), Math.max(100, e.getProcessedCount() / 10));
            
        Map<String, Object> response = createErrorResponse(
            e.getErrorCode(),
            e.getMessage(),
            suggestion,
            request
        );
        response.put("indexName", e.getIndexName());
        response.put("processedCount", e.getProcessedCount());
        response.put("failedCount", e.getFailedCount());
        response.put("successRate", e.getProcessedCount() > 0 ? 
            String.format("%.2f%%", (double)(e.getProcessedCount() - e.getFailedCount()) / e.getProcessedCount() * 100) : "0%");
        
        if (e.getBatchId() != null) {
            response.put("batchId", e.getBatchId());
        }
        response.put("exceptionType", "DATA_PROCESSING_ERROR");
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
    
    // ========== 网络和连接异常处理 ==========
    
    @ExceptionHandler({ConnectException.class, SocketTimeoutException.class})
    public ResponseEntity<Map<String, Object>> handleNetworkException(Exception e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        networkExceptions.incrementAndGet();
        
        log.error("网络连接异常: {}", e.getMessage(), e);
        
        String suggestion = "网络连接超时或失败，请检查：1)ES服务器是否启动 2)网络连接是否正常 3)防火墙配置 4)服务器负载情况";
        
        Map<String, Object> response = createErrorResponse(
            "NETWORK_CONNECTION_FAILED",
            "网络连接异常: " + e.getMessage(),
            suggestion,
            request
        );
        response.put("exceptionType", "NETWORK_ERROR");
        
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
    }
    
    @ExceptionHandler(IOException.class)
    public ResponseEntity<Map<String, Object>> handleIOException(IOException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        systemExceptions.incrementAndGet();
        
        log.error("IO异常: {}", e.getMessage(), e);
        
        Map<String, Object> response = createErrorResponse(
            "IO_ERROR",
            "输入输出异常: " + e.getMessage(),
            "可能是文件读写错误或网络IO问题，请检查磁盘空间、文件权限或网络状态",
            request
        );
        response.put("exceptionType", "IO_ERROR");
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
    
    // ========== 参数验证异常处理 ==========
    
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidationException(MethodArgumentNotValidException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        validationExceptions.incrementAndGet();
        
        log.error("参数验证失败: {}", e.getMessage());
        
        StringBuilder errorDetails = new StringBuilder();
        for (FieldError error : e.getBindingResult().getFieldErrors()) {
            errorDetails.append(String.format("字段'%s': %s; ", error.getField(), error.getDefaultMessage()));
        }
        
        Map<String, Object> response = createErrorResponse(
            "VALIDATION_ERROR",
            "参数验证失败: " + errorDetails.toString(),
            "请检查请求参数格式和必填项是否正确",
            request
        );
        response.put("fieldErrors", e.getBindingResult().getFieldErrors());
        response.put("exceptionType", "VALIDATION_ERROR");
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    @ExceptionHandler(BindException.class)
    public ResponseEntity<Map<String, Object>> handleBindException(BindException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        validationExceptions.incrementAndGet();
        
        log.error("数据绑定异常: {}", e.getMessage());
        
        Map<String, Object> response = createErrorResponse(
            "BIND_ERROR",
            "数据绑定失败: " + e.getMessage(),
            "请检查请求数据格式是否与预期的数据结构匹配",
            request
        );
        response.put("exceptionType", "VALIDATION_ERROR");
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<Map<String, Object>> handleMissingParameter(MissingServletRequestParameterException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        validationExceptions.incrementAndGet();
        
        log.error("缺少请求参数: {}", e.getMessage());
        
        Map<String, Object> response = createErrorResponse(
            "MISSING_PARAMETER",
            String.format("缺少必需的请求参数: %s (%s)", e.getParameterName(), e.getParameterType()),
            String.format("请在请求中包含参数 '%s'", e.getParameterName()),
            request
        );
        response.put("parameterName", e.getParameterName());
        response.put("parameterType", e.getParameterType());
        response.put("exceptionType", "VALIDATION_ERROR");
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<Map<String, Object>> handleTypeMismatch(MethodArgumentTypeMismatchException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        validationExceptions.incrementAndGet();
        
        log.error("参数类型转换异常: {}", e.getMessage());
        
        Map<String, Object> response = createErrorResponse(
            "TYPE_MISMATCH",
            String.format("参数 '%s' 的值 '%s' 无法转换为所需类型 %s", 
                e.getName(), e.getValue(), e.getRequiredType().getSimpleName()),
            String.format("请检查参数 '%s' 的格式是否正确", e.getName()),
            request
        );
        response.put("parameterName", e.getName());
        response.put("providedValue", e.getValue());
        response.put("requiredType", e.getRequiredType().getSimpleName());
        response.put("exceptionType", "VALIDATION_ERROR");
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> handleMessageNotReadable(HttpMessageNotReadableException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        validationExceptions.incrementAndGet();
        
        log.error("HTTP消息不可读异常: {}", e.getMessage());
        
        Map<String, Object> response = createErrorResponse(
            "MESSAGE_NOT_READABLE",
            "请求体格式错误或不可读",
            "请检查JSON格式是否正确，字段名是否拼写正确，数据类型是否匹配",
            request
        );
        response.put("exceptionType", "VALIDATION_ERROR");
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    // ========== HTTP方法和路径异常处理 ==========
    
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<Map<String, Object>> handleMethodNotSupported(HttpRequestMethodNotSupportedException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        validationExceptions.incrementAndGet();
        
        log.error("HTTP方法不支持异常: {}", e.getMessage());
        
        Map<String, Object> response = createErrorResponse(
            "METHOD_NOT_SUPPORTED",
            String.format("请求方法 '%s' 不被支持", e.getMethod()),
            String.format("支持的方法: %s", String.join(", ", e.getSupportedMethods())),
            request
        );
        response.put("requestMethod", e.getMethod());
        response.put("supportedMethods", e.getSupportedMethods());
        response.put("exceptionType", "CLIENT_ERROR");
        
        return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).body(response);
    }
    
    @ExceptionHandler(NoHandlerFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNoHandlerFound(NoHandlerFoundException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        validationExceptions.incrementAndGet();
        
        log.error("未找到请求处理器: {} {}", e.getHttpMethod(), e.getRequestURL());
        
        Map<String, Object> response = createErrorResponse(
            "ENDPOINT_NOT_FOUND",
            String.format("未找到路径 '%s' 的 %s 请求处理器", e.getRequestURL(), e.getHttpMethod()),
            "请检查请求URL是否正确，或查看API文档了解正确的接口地址",
            request
        );
        response.put("requestMethod", e.getHttpMethod());
        response.put("requestURL", e.getRequestURL());
        response.put("exceptionType", "CLIENT_ERROR");
        
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
    }
    
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgumentException(IllegalArgumentException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        validationExceptions.incrementAndGet();
        
        log.error("参数异常: {}", e.getMessage());
        
        Map<String, Object> response = createErrorResponse(
            "INVALID_PARAMETER",
            "参数错误: " + e.getMessage(),
            "请检查请求参数的值是否在有效范围内",
            request
        );
        response.put("exceptionType", "VALIDATION_ERROR");
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    // ========== 线程和并发异常处理 ==========
    
    @ExceptionHandler(InterruptedException.class)
    public ResponseEntity<Map<String, Object>> handleInterruptedException(InterruptedException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        systemExceptions.incrementAndGet();
        
        log.error("线程中断异常: {}", e.getMessage(), e);
        
        // 恢复中断状态
        Thread.currentThread().interrupt();
        
        Map<String, Object> response = createErrorResponse(
            "THREAD_INTERRUPTED",
            "操作被中断: " + e.getMessage(),
            "任务执行被中断，可能是用户取消操作或系统关闭，请重新尝试",
            request
        );
        response.put("exceptionType", "INTERRUPTION_ERROR");
        
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
    }
    
    // ========== 通用异常处理 ==========
    
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Map<String, Object>> handleRuntimeException(RuntimeException e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        systemExceptions.incrementAndGet();
        
        log.error("运行时异常: {} - {}", e.getClass().getSimpleName(), e.getMessage(), e);
        
        Map<String, Object> response = createErrorResponse(
            "RUNTIME_ERROR",
            "系统内部错误: " + e.getMessage(),
            "系统遇到运行时异常，请检查操作是否合理，或联系管理员",
            request
        );
        response.put("exceptionClass", e.getClass().getSimpleName());
        response.put("exceptionType", "SYSTEM_ERROR");
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
    
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneralException(Exception e, HttpServletRequest request) {
        totalExceptions.incrementAndGet();
        systemExceptions.incrementAndGet();
        
        log.error("未知异常: {} - {}", e.getClass().getSimpleName(), e.getMessage(), e);
        
        Map<String, Object> response = createErrorResponse(
            "UNKNOWN_ERROR",
            "系统发生未知错误: " + e.getMessage(),
            "系统遇到未预期的异常，请记录错误追踪ID并联系技术支持",
            request
        );
        response.put("exceptionClass", e.getClass().getSimpleName());
        response.put("exceptionType", "UNKNOWN_ERROR");
        
        // 为未知异常提供更多调试信息（仅在开发环境）
        String profiles = System.getProperty("spring.profiles.active", "");
        if (profiles.contains("dev") || profiles.contains("test")) {
            response.put("stackTrace", e.getStackTrace());
        }
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}