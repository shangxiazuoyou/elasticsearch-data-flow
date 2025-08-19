package com.everflowx.esmigration.resilience;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * 熔断器管理器 - 防止系统雪崩，确保大数据量迁移的稳定性
 * 
 * 熔断状态：
 * - CLOSED: 正常工作状态
 * - OPEN: 熔断开启，拒绝请求
 * - HALF_OPEN: 半开状态，尝试恢复
 * 
 * @author everflowx
 */
@Slf4j
@Component
public class CircuitBreakerManager {
    
    private final ConcurrentHashMap<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();
    
    // 默认配置
    private static final int DEFAULT_FAILURE_THRESHOLD = 50; // 失败阈值：50次
    private static final double DEFAULT_FAILURE_RATE_THRESHOLD = 0.5; // 失败率阈值：50%
    private static final long DEFAULT_TIMEOUT = 60000; // 熔断超时：60秒
    private static final int DEFAULT_MIN_REQUESTS = 20; // 最小请求数：20次
    
    /**
     * 获取或创建熔断器
     */
    public CircuitBreaker getOrCreateBreaker(String name) {
        return breakers.computeIfAbsent(name, k -> new CircuitBreaker(
            name, DEFAULT_FAILURE_THRESHOLD, DEFAULT_FAILURE_RATE_THRESHOLD, 
            DEFAULT_TIMEOUT, DEFAULT_MIN_REQUESTS));
    }
    
    /**
     * 获取自定义配置的熔断器
     */
    public CircuitBreaker getOrCreateBreaker(String name, int failureThreshold, 
                                           double failureRateThreshold, long timeoutMs, int minRequests) {
        return breakers.computeIfAbsent(name, k -> new CircuitBreaker(
            name, failureThreshold, failureRateThreshold, timeoutMs, minRequests));
    }
    
    /**
     * 执行带熔断保护的操作
     */
    public <T> T executeWithBreaker(String breakerName, Supplier<T> operation, Supplier<T> fallback) {
        CircuitBreaker breaker = getOrCreateBreaker(breakerName);
        return breaker.execute(operation, fallback);
    }
    
    /**
     * 获取所有熔断器状态
     */
    public String getAllBreakerStatus() {
        StringBuilder status = new StringBuilder("熔断器状态报告:\n");
        breakers.forEach((name, breaker) -> {
            status.append(String.format("- %s: %s (成功: %d, 失败: %d, 失败率: %.2f%%)\n",
                name, breaker.getState(), breaker.getSuccessCount(), 
                breaker.getFailureCount(), breaker.getFailureRate() * 100));
        });
        return status.toString();
    }
    
    /**
     * 重置所有熔断器
     */
    public void resetAllBreakers() {
        breakers.values().forEach(CircuitBreaker::reset);
        log.info("所有熔断器已重置");
    }
    
    /**
     * 熔断器实现
     */
    public static class CircuitBreaker {
        private final String name;
        private final int failureThreshold;
        private final double failureRateThreshold;
        private final long timeoutMs;
        private final int minRequests;
        
        private volatile CircuitBreakerState state = CircuitBreakerState.CLOSED;
        private final AtomicInteger successCount = new AtomicInteger(0);
        private final AtomicInteger failureCount = new AtomicInteger(0);
        private final AtomicLong lastFailureTime = new AtomicLong(0);
        private final AtomicLong stateChangeTime = new AtomicLong(System.currentTimeMillis());
        
        public CircuitBreaker(String name, int failureThreshold, double failureRateThreshold, 
                            long timeoutMs, int minRequests) {
            this.name = name;
            this.failureThreshold = failureThreshold;
            this.failureRateThreshold = failureRateThreshold;
            this.timeoutMs = timeoutMs;
            this.minRequests = minRequests;
            log.info("创建熔断器: {} - 失败阈值: {}, 失败率阈值: {:.1f}%, 超时: {}ms, 最小请求数: {}", 
                name, failureThreshold, failureRateThreshold * 100, timeoutMs, minRequests);
        }
        
        /**
         * 执行操作
         */
        public <T> T execute(Supplier<T> operation, Supplier<T> fallback) {
            if (state == CircuitBreakerState.OPEN) {
                if (shouldAttemptReset()) {
                    state = CircuitBreakerState.HALF_OPEN;
                    stateChangeTime.set(System.currentTimeMillis());
                    log.info("熔断器 {} 进入半开状态", name);
                } else {
                    log.debug("熔断器 {} 处于开启状态，执行降级逻辑", name);
                    return fallback.get();
                }
            }
            
            try {
                T result = operation.get();
                onSuccess();
                return result;
            } catch (Exception e) {
                onFailure();
                if (state == CircuitBreakerState.OPEN) {
                    log.warn("熔断器 {} 执行失败，使用降级逻辑: {}", name, e.getMessage());
                    return fallback.get();
                }
                throw e;
            }
        }
        
        /**
         * 记录成功
         */
        private void onSuccess() {
            successCount.incrementAndGet();
            
            if (state == CircuitBreakerState.HALF_OPEN) {
                // 半开状态下成功，重置为关闭状态
                reset();
                log.info("熔断器 {} 恢复正常，重置为关闭状态", name);
            }
        }
        
        /**
         * 记录失败
         */
        private void onFailure() {
            failureCount.incrementAndGet();
            lastFailureTime.set(System.currentTimeMillis());
            
            if (shouldTripBreaker()) {
                state = CircuitBreakerState.OPEN;
                stateChangeTime.set(System.currentTimeMillis());
                log.warn("熔断器 {} 触发开启 - 失败次数: {}, 失败率: {:.2f}%", 
                    name, failureCount.get(), getFailureRate() * 100);
            }
        }
        
        /**
         * 是否应该触发熔断
         */
        private boolean shouldTripBreaker() {
            int totalRequests = successCount.get() + failureCount.get();
            
            // 请求数不够，不触发熔断
            if (totalRequests < minRequests) {
                return false;
            }
            
            // 检查失败次数阈值
            if (failureCount.get() >= failureThreshold) {
                return true;
            }
            
            // 检查失败率阈值
            double currentFailureRate = getFailureRate();
            return currentFailureRate >= failureRateThreshold;
        }
        
        /**
         * 是否应该尝试重置
         */
        private boolean shouldAttemptReset() {
            return System.currentTimeMillis() - stateChangeTime.get() >= timeoutMs;
        }
        
        /**
         * 重置熔断器
         */
        public void reset() {
            state = CircuitBreakerState.CLOSED;
            successCount.set(0);
            failureCount.set(0);
            stateChangeTime.set(System.currentTimeMillis());
        }
        
        /**
         * 获取失败率
         */
        public double getFailureRate() {
            int total = successCount.get() + failureCount.get();
            return total == 0 ? 0.0 : (double) failureCount.get() / total;
        }
        
        // Getters
        public CircuitBreakerState getState() { return state; }
        public int getSuccessCount() { return successCount.get(); }
        public int getFailureCount() { return failureCount.get(); }
        public String getName() { return name; }
        
        /**
         * 强制开启熔断器
         */
        public void forceOpen() {
            state = CircuitBreakerState.OPEN;
            stateChangeTime.set(System.currentTimeMillis());
            log.warn("熔断器 {} 被强制开启", name);
        }
        
        /**
         * 强制关闭熔断器
         */
        public void forceClose() {
            reset();
            log.info("熔断器 {} 被强制关闭", name);
        }
    }
    
    /**
     * 熔断器状态枚举
     */
    public enum CircuitBreakerState {
        CLOSED,   // 关闭状态（正常）
        OPEN,     // 开启状态（熔断）
        HALF_OPEN // 半开状态（尝试恢复）
    }
    
    /**
     * 熔断器配置
     */
    @Data
    public static class CircuitBreakerConfig {
        private int failureThreshold = DEFAULT_FAILURE_THRESHOLD;
        private double failureRateThreshold = DEFAULT_FAILURE_RATE_THRESHOLD;
        private long timeoutMs = DEFAULT_TIMEOUT;
        private int minRequests = DEFAULT_MIN_REQUESTS;
    }
}