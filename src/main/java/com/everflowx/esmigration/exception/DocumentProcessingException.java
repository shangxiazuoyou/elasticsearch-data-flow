package com.everflowx.esmigration.exception;

/**
 * 文档处理异常
 * 用于表示单个文档在处理过程中的异常
 * 
 * @author everflowx
 */
public class DocumentProcessingException extends EsMigrationException {
    
    private String documentId;
    private String processingStep;
    private String originalSource;
    
    public DocumentProcessingException(String errorCode, String message, String documentId, 
                                     String processingStep) {
        super(errorCode, message);
        this.documentId = documentId;
        this.processingStep = processingStep;
    }
    
    public DocumentProcessingException(String errorCode, String message, String documentId, 
                                     String processingStep, Throwable cause) {
        super(errorCode, message, cause);
        this.documentId = documentId;
        this.processingStep = processingStep;
    }
    
    /**
     * 静态工厂方法：JSON解析失败
     */
    public static DocumentProcessingException jsonParseError(String documentId, String jsonSource, Throwable cause) {
        DocumentProcessingException ex = new DocumentProcessingException(
            "JSON_PARSE_ERROR", 
            "文档JSON解析失败: " + cause.getMessage(), 
            documentId, 
            "JSON_PARSING", 
            cause
        );
        ex.originalSource = jsonSource;
        return ex;
    }
    
    /**
     * 静态工厂方法：字段映射失败
     */
    public static DocumentProcessingException fieldMappingError(String documentId, String fieldName, 
                                                              Object fieldValue, Throwable cause) {
        return new DocumentProcessingException(
            "FIELD_MAPPING_ERROR", 
            String.format("文档字段映射失败，字段名: %s, 字段值: %s, 错误: %s", 
                fieldName, fieldValue, cause.getMessage()), 
            documentId, 
            "FIELD_MAPPING", 
            cause
        );
    }
    
    /**
     * 静态工厂方法：数据转换失败
     */
    public static DocumentProcessingException dataTransformError(String documentId, String transformType, Throwable cause) {
        return new DocumentProcessingException(
            "DATA_TRANSFORM_ERROR", 
            "文档数据转换失败，转换类型: " + transformType + ", 错误: " + cause.getMessage(), 
            documentId, 
            "DATA_TRANSFORM", 
            cause
        );
    }
    
    /**
     * 静态工厂方法：验证失败
     */
    public static DocumentProcessingException validationError(String documentId, String validationRule, String reason) {
        return new DocumentProcessingException(
            "DOCUMENT_VALIDATION_ERROR", 
            "文档验证失败，验证规则: " + validationRule + ", 原因: " + reason, 
            documentId, 
            "VALIDATION"
        );
    }
    
    public String getDocumentId() {
        return documentId;
    }
    
    public String getProcessingStep() {
        return processingStep;
    }
    
    public String getOriginalSource() {
        return originalSource;
    }
    
    @Override
    public String toString() {
        return String.format("DocumentProcessingException{errorCode='%s', message='%s', documentId='%s', processingStep='%s'}", 
            getErrorCode(), getMessage(), documentId, processingStep);
    }
}