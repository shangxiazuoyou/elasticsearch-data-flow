package com.everflowx.esmigration.throttle;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * 自适应限流控制器 - 确保无论多大数据量都能稳定迁移
 * 
 * 核心特性：
 * 1. 内存压力感知：动态调整处理速度
 * 2. 自适应限流：根据系统负载自动调节
 * 3. 回压机制：防止下游处理不及时导致的内存堆积
 * 4. 多级限流：批次级、文档级、时间级多重保护
 * 5. 优雅降级：保证系统稳定性优于处理速度
 * 
 * @author everflowx
 */
@Slf4j
@Component
public class AdaptiveThrottleController {
    
    // 内存压力阈值
    private static final double MEMORY_GREEN = 0.7;    // 绿色区域：正常处理
    private static final double MEMORY_YELLOW = 0.8;   // 黄色区域：开始限流
    private static final double MEMORY_RED = 0.9;      // 红色区域：严重限流
    private static final double MEMORY_CRITICAL = 0.95; // 危险区域：暂停处理
    
    // 限流参数
    private final Semaphore processingPermits;
    private final AtomicInteger maxConcurrentBatches = new AtomicInteger(10);
    private final AtomicLong totalProcessedDocs = new AtomicLong(0);
    private final AtomicLong totalProcessingTime = new AtomicLong(0);
    private final DoubleAdder currentThroughput = new DoubleAdder();
    
    // 自适应参数
    private volatile int currentBatchSize = 1000;
    private volatile int baseBatchSize = 1000;
    private volatile long lastAdjustmentTime = System.currentTimeMillis();
    private volatile double lastMemoryPressure = 0.0;
    
    // 回压控制
    private final AtomicInteger queueSize = new AtomicInteger(0);
    private final AtomicInteger maxQueueSize = new AtomicInteger(100);
    
    public AdaptiveThrottleController() {
        this.processingPermits = new Semaphore(maxConcurrentBatches.get());
    }
    
    /**
     * 获取处理许可证 - 带自适应控制
     */
    public ThrottlePermit acquirePermit() throws InterruptedException {
        // 检查内存压力
        double memoryPressure = getMemoryPressure();
        
        // 危险区域：暂停所有处理
        if (memoryPressure > MEMORY_CRITICAL) {
            log.warn("内存压力达到危险水平 {:.1f}%，暂停所有处理", memoryPressure * 100);
            performEmergencyCleanup();
            Thread.sleep(5000); // 等待5秒
            return acquirePermit(); // 递归重试
        }
        
        // 红色区域：严重限流
        if (memoryPressure > MEMORY_RED) {
            log.warn("内存压力过高 {:.1f}%，启动严重限流", memoryPressure * 100);
            adjustForHighPressure();
            Thread.sleep(1000);
        }
        // 黄色区域：适度限流
        else if (memoryPressure > MEMORY_YELLOW) {
            log.debug("内存压力较高 {:.1f}%，启动适度限流", memoryPressure * 100);
            adjustForModeratePressure();
            Thread.sleep(100);
        }
        // 绿色区域：正常处理
        else if (memoryPressure < MEMORY_GREEN) {
            adjustForLowPressure();
        }
        
        // 获取处理许可
        processingPermits.acquire();
        queueSize.incrementAndGet();
        
        return new ThrottlePermit(this, memoryPressure);
    }
    
    /**
     * 释放处理许可证
     */
    public void releasePermit(ThrottlePermit permit) {
        processingPermits.release();
        queueSize.decrementAndGet();
        
        // 更新吞吐量统计
        long processingTime = System.currentTimeMillis() - permit.getStartTime();
        totalProcessingTime.addAndGet(processingTime);
        currentThroughput.add(permit.getProcessedDocs());
        
        // 定期调整参数
        if (System.currentTimeMillis() - lastAdjustmentTime > 10000) { // 每10秒调整一次
            adaptParameters();
        }
    }
    
    /**
     * 获取自适应的批次大小
     */
    public int getAdaptiveBatchSize() {
        double memoryPressure = getMemoryPressure();
        
        if (memoryPressure > MEMORY_RED) {
            return Math.max(currentBatchSize / 4, 50); // 最小50
        } else if (memoryPressure > MEMORY_YELLOW) {
            return Math.max(currentBatchSize / 2, 100); // 最小100
        } else if (memoryPressure < MEMORY_GREEN && currentBatchSize < baseBatchSize * 2) {
            return Math.min(currentBatchSize * 2, 5000); // 最大5000
        }
        
        return currentBatchSize;
    }
    
    /**
     * 检查是否应该暂停处理
     */
    public boolean shouldPause() {
        double memoryPressure = getMemoryPressure();
        int currentQueueSize = queueSize.get();
        
        // 内存压力过高或队列过满时暂停
        return memoryPressure > MEMORY_CRITICAL || currentQueueSize > maxQueueSize.get();
    }
    
    /**
     * 获取推荐的并发数
     */
    public int getRecommendedConcurrency() {
        double memoryPressure = getMemoryPressure();
        int baseConcurrency = Runtime.getRuntime().availableProcessors();
        
        if (memoryPressure > MEMORY_RED) {
            return Math.max(baseConcurrency / 4, 1);
        } else if (memoryPressure > MEMORY_YELLOW) {
            return Math.max(baseConcurrency / 2, 2);
        } else {
            return baseConcurrency;
        }
    }
    
    /**
     * 高压力调整
     */
    private void adjustForHighPressure() {
        // 减少并发数
        int newConcurrency = Math.max(maxConcurrentBatches.get() / 2, 1);
        maxConcurrentBatches.set(newConcurrency);
        
        // 减少批次大小
        currentBatchSize = Math.max(currentBatchSize / 2, 50);
        
        // 减少队列大小
        maxQueueSize.set(Math.max(maxQueueSize.get() / 2, 10));
        
        log.debug("高压力调整 - 并发: {}, 批次: {}, 队列: {}", 
            newConcurrency, currentBatchSize, maxQueueSize.get());
    }
    
    /**
     * 中等压力调整
     */
    private void adjustForModeratePressure() {
        // 适度减少并发数
        int newConcurrency = Math.max(maxConcurrentBatches.get() * 3 / 4, 2);
        maxConcurrentBatches.set(newConcurrency);
        
        // 适度减少批次大小
        currentBatchSize = Math.max((int)(currentBatchSize * 0.8), 100);
        
        log.debug("中压力调整 - 并发: {}, 批次: {}", newConcurrency, currentBatchSize);
    }
    
    /**
     * 低压力调整（增加处理能力）
     */
    private void adjustForLowPressure() {
        // 适当增加并发数
        int maxPossible = Runtime.getRuntime().availableProcessors() * 2;
        int newConcurrency = Math.min(maxConcurrentBatches.get() + 1, maxPossible);
        maxConcurrentBatches.set(newConcurrency);
        
        // 适当增加批次大小
        currentBatchSize = Math.min(currentBatchSize + 100, baseBatchSize * 2);
        
        // 适当增加队列大小
        maxQueueSize.set(Math.min(maxQueueSize.get() + 10, 200));
        
        log.debug("低压力调整 - 并发: {}, 批次: {}, 队列: {}", 
            newConcurrency, currentBatchSize, maxQueueSize.get());
    }
    
    /**
     * 紧急清理
     */
    private void performEmergencyCleanup() {
        log.warn("执行紧急内存清理");
        
        // 强制GC
        System.gc();
        
        // 等待GC完成
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // 重置为最保守的参数
        maxConcurrentBatches.set(1);
        currentBatchSize = 50;
        maxQueueSize.set(5);
        
        log.warn("紧急清理完成，参数重置为最保守值");
    }
    
    /**
     * 自适应参数调整
     */
    private void adaptParameters() {
        lastAdjustmentTime = System.currentTimeMillis();
        double memoryPressure = getMemoryPressure();
        
        // 计算处理效率
        long avgProcessingTime = totalProcessedDocs.get() > 0 ? 
            totalProcessingTime.get() / totalProcessedDocs.get() : 0;
        
        double memoryTrend = memoryPressure - lastMemoryPressure;
        lastMemoryPressure = memoryPressure;
        
        // 根据内存趋势调整
        if (memoryTrend > 0.05) { // 内存压力快速上升
            log.info("内存压力快速上升，主动降低处理强度");
            adjustForHighPressure();
        } else if (memoryTrend < -0.05 && memoryPressure < MEMORY_YELLOW) { // 内存压力快速下降
            log.info("内存压力下降，适当提高处理强度");
            adjustForLowPressure();
        }
        
        // 输出统计信息
        log.info("自适应调整 - 内存压力: {:.1f}%, 并发: {}, 批次: {}, 队列: {}, 平均处理时间: {}ms", 
            memoryPressure * 100, maxConcurrentBatches.get(), currentBatchSize, 
            maxQueueSize.get(), avgProcessingTime);
    }
    
    /**
     * 获取内存压力值
     */
    private double getMemoryPressure() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long maxMemory = runtime.maxMemory();
        
        // 计算已使用内存占最大内存的比例
        long usedMemory = totalMemory - freeMemory;
        return (double) usedMemory / maxMemory;
    }
    
    /**
     * 获取系统统计信息
     */
    public ThrottleStats getStats() {
        ThrottleStats stats = new ThrottleStats();
        stats.setMemoryPressure(getMemoryPressure());
        stats.setCurrentBatchSize(currentBatchSize);
        stats.setMaxConcurrentBatches(maxConcurrentBatches.get());
        stats.setCurrentQueueSize(queueSize.get());
        stats.setMaxQueueSize(maxQueueSize.get());
        stats.setTotalProcessedDocs(totalProcessedDocs.get());
        stats.setTotalProcessingTimeMs(totalProcessingTime.get());
        stats.setCurrentThroughput(currentThroughput.sum());
        return stats;
    }
    
    /**
     * 重置统计信息
     */
    public void resetStats() {
        totalProcessedDocs.set(0);
        totalProcessingTime.set(0);
        currentThroughput.reset();
        log.info("限流控制器统计信息已重置");
    }
    
    /**
     * 强制设置参数（紧急情况）
     */
    public void forceSetParameters(int batchSize, int concurrency, int queueSize) {
        this.currentBatchSize = Math.max(batchSize, 10);
        this.maxConcurrentBatches.set(Math.max(concurrency, 1));
        this.maxQueueSize.set(Math.max(queueSize, 1));
        
        log.warn("强制设置参数 - 批次: {}, 并发: {}, 队列: {}", 
            this.currentBatchSize, concurrency, queueSize);
    }
    
    /**
     * 限流许可证
     */
    @Data
    public static class ThrottlePermit {
        private final AdaptiveThrottleController controller;
        private final double memoryPressureAtAcquire;
        private final long startTime;
        private volatile int processedDocs = 0;
        
        public ThrottlePermit(AdaptiveThrottleController controller, double memoryPressure) {
            this.controller = controller;
            this.memoryPressureAtAcquire = memoryPressure;
            this.startTime = System.currentTimeMillis();
        }
        
        public void updateProcessedDocs(int docs) {
            this.processedDocs += docs;
            controller.totalProcessedDocs.addAndGet(docs);
        }
        
        public void release() {
            controller.releasePermit(this);
        }
        
        /**
         * 检查是否应该在处理中暂停
         */
        public boolean shouldPauseProcessing() {
            return controller.shouldPause();
        }
        
        /**
         * 获取当前推荐的批次大小
         */
        public int getCurrentBatchSize() {
            return controller.getAdaptiveBatchSize();
        }
    }
    
    /**
     * 限流统计信息
     */
    @Data
    public static class ThrottleStats {
        private double memoryPressure;
        private int currentBatchSize;
        private int maxConcurrentBatches;
        private int currentQueueSize;
        private int maxQueueSize;
        private long totalProcessedDocs;
        private long totalProcessingTimeMs;
        private double currentThroughput;
        
        public double getAverageProcessingTime() {
            return totalProcessedDocs > 0 ? (double) totalProcessingTimeMs / totalProcessedDocs : 0;
        }
        
        public double getProcessingSpeed() {
            return totalProcessingTimeMs > 0 ? (double) totalProcessedDocs / totalProcessingTimeMs * 1000 : 0;
        }
        
        @Override
        public String toString() {
            return String.format(
                "ThrottleStats{内存压力=%.1f%%, 批次大小=%d, 并发数=%d, 队列=%d/%d, " +
                "已处理=%d, 平均处理时间=%.1fms, 处理速度=%.1f docs/s}",
                memoryPressure * 100, currentBatchSize, maxConcurrentBatches,
                currentQueueSize, maxQueueSize, totalProcessedDocs,
                getAverageProcessingTime(), getProcessingSpeed()
            );
        }
    }
}