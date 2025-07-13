package com.everflowx.esmigration.exception;

/**
 * ES连接异常
 * 
 * @author everflowx
 */
public class EsConnectionException extends EsMigrationException {
    
    private String esHost;
    private int esPort;
    private boolean isTarget;
    
    public EsConnectionException(String message, String esHost, int esPort, boolean isTarget) {
        super("CONNECTION_ERROR", message);
        this.esHost = esHost;
        this.esPort = esPort;
        this.isTarget = isTarget;
    }
    
    public EsConnectionException(String message, String esHost, int esPort, boolean isTarget, Throwable cause) {
        super("CONNECTION_ERROR", message, null, cause);
        this.esHost = esHost;
        this.esPort = esPort;
        this.isTarget = isTarget;
    }
    
    // 为ExceptionHandler提供的构造函数
    public EsConnectionException(String errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
        this.esHost = "unknown";
        this.esPort = 0;
        this.isTarget = false;
    }
    
    public String getEsHost() {
        return esHost;
    }
    
    public int getEsPort() {
        return esPort;
    }
    
    public boolean isTarget() {
        return isTarget;
    }
    
    public String getEsType() {
        return isTarget ? "目标ES" : "源ES";
    }
    
    @Override
    public String toString() {
        return String.format("EsConnectionException{%s连接失败: %s:%d, message='%s'}", 
                           getEsType(), esHost, esPort, getMessage());
    }
    
    public static EsConnectionException connectionTimeout(String esHost, int esPort, boolean isTarget) {
        return new EsConnectionException(
            String.format("%s连接超时: %s:%d", isTarget ? "目标ES" : "源ES", esHost, esPort),
            esHost, esPort, isTarget
        );
    }
    
    public static EsConnectionException authenticationFailed(String esHost, int esPort, boolean isTarget) {
        return new EsConnectionException(
            String.format("%s认证失败: %s:%d", isTarget ? "目标ES" : "源ES", esHost, esPort),
            esHost, esPort, isTarget
        );
    }
    
    public static EsConnectionException connectionRefused(String esHost, int esPort, boolean isTarget) {
        return new EsConnectionException(
            String.format("%s连接被拒绝: %s:%d", isTarget ? "目标ES" : "源ES", esHost, esPort),
            esHost, esPort, isTarget
        );
    }
}