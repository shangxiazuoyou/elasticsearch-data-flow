package com.everflowx.esmigration.exception;

/**
 * 迁移配置异常
 * 
 * @author everflowx
 */
public class MigrationConfigException extends EsMigrationException {
    
    public MigrationConfigException(String message) {
        super("CONFIG_ERROR", message);
    }
    
    public MigrationConfigException(String message, String fieldName) {
        super("CONFIG_ERROR", 
              String.format("配置错误 [%s]: %s", fieldName, message));
    }
    
    public static MigrationConfigException invalidBatchSize(int batchSize) {
        return new MigrationConfigException(
            String.format("批次大小无效: %d, 必须在1-10000之间", batchSize),
            "batchSize"
        );
    }
    
    public static MigrationConfigException invalidScrollTimeout(int timeout) {
        return new MigrationConfigException(
            String.format("滚动超时时间无效: %d分钟, 必须在1-60分钟之间", timeout),
            "scrollTimeout"
        );
    }
    
    public static MigrationConfigException invalidThreadCount(int threadCount) {
        return new MigrationConfigException(
            String.format("线程数无效: %d, 必须在1-10之间", threadCount),
            "threadCount"
        );
    }
    
    public static MigrationConfigException missingRequiredField(String fieldName) {
        return new MigrationConfigException(
            String.format("缺少必需的配置字段: %s", fieldName),
            fieldName
        );
    }
}