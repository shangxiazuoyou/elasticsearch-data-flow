package com.everflowx.esmigration.exception;

/**
 * ES数据迁移异常基类
 * 
 * @author everflowx
 */
public class EsMigrationException extends RuntimeException {
    
    private String errorCode;
    private String indexName;
    
    public EsMigrationException(String message) {
        super(message);
    }
    
    public EsMigrationException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public EsMigrationException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }
    
    public EsMigrationException(String errorCode, String message, String indexName) {
        super(message);
        this.errorCode = errorCode;
        this.indexName = indexName;
    }
    
    public EsMigrationException(String errorCode, String message, String indexName, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.indexName = indexName;
    }
    
    public EsMigrationException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }
    
    public String getErrorCode() {
        return errorCode;
    }
    
    public String getIndexName() {
        return indexName;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("EsMigrationException{");
        if (errorCode != null) {
            sb.append("errorCode='").append(errorCode).append("'");
        }
        if (indexName != null) {
            sb.append(", indexName='").append(indexName).append("'");
        }
        sb.append(", message='").append(getMessage()).append("'");
        sb.append("}");
        return sb.toString();
    }
}