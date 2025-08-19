package com.everflowx.esmigration.stream;

import com.everflowx.esmigration.domain.MigrationConfig;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 流式迁移引擎 - 支持无限大数据量的稳定迁移
 * 关键特性：
 * 1. 流式处理：数据流动，不在内存中累积
 * 2. 背压控制：自动调节生产和消费速度
 * 3. 内存安全：严格控制内存使用
 * 4. 失败隔离：单个文档失败不影响整体
 * 
 * @author everflowx
 */
@Slf4j
@Component
public class StreamingMigrationEngine {
    
    // 内存压力阈值：超过80%开始限流，超过90%暂停处理
    private static final double MEMORY_PRESSURE_THROTTLE = 0.8;
    private static final double MEMORY_PRESSURE_PAUSE = 0.9;
    
    // 流式处理配置
    private static final int QUEUE_CAPACITY = 10000; // 队列容量
    private static final int MAX_CONCURRENT_BATCHES = 5; // 最大并发批次
    
    /**
     * 启动流式迁移
     */
    public StreamingMigrationResult startStreaming(
            RestHighLevelClient sourceClient,
            RestHighLevelClient targetClient,
            MigrationConfig config,
            DocumentSupplier supplier,
            DocumentTransformer transformer) {
        
        StreamingMigrationResult result = new StreamingMigrationResult();
        
        // 创建有界队列，防止内存无限增长
        BlockingQueue<SearchHit[]> batchQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        
        // 信号量控制并发批次数量
        Semaphore batchSemaphore = new Semaphore(MAX_CONCURRENT_BATCHES);
        
        AtomicBoolean producerFinished = new AtomicBoolean(false);
        AtomicBoolean consumerStopped = new AtomicBoolean(false);
        
        // 启动生产者线程（读取数据）
        Thread producer = new Thread(() -> {
            try {
                produceData(supplier, batchQueue, config.getBatchSize(), producerFinished, result);
            } catch (Exception e) {
                log.error("生产者线程异常", e);
                result.setErrorMessage("生产者异常: " + e.getMessage());
                producerFinished.set(true);
            }
        }, "StreamingProducer");
        
        // 启动消费者线程（处理数据）
        Thread consumer = new Thread(() -> {
            try {
                consumeData(targetClient, batchQueue, transformer, config, 
                    producerFinished, consumerStopped, batchSemaphore, result);
            } catch (Exception e) {
                log.error("消费者线程异常", e);
                result.setErrorMessage("消费者异常: " + e.getMessage());
                consumerStopped.set(true);
            }
        }, "StreamingConsumer");
        
        // 启动监控线程
        Thread monitor = new Thread(() -> {
            monitorMemoryAndFlow(batchQueue, result, producerFinished, consumerStopped);
        }, "StreamingMonitor");
        
        log.info("启动流式迁移引擎 - 队列容量: {}, 最大并发批次: {}", QUEUE_CAPACITY, MAX_CONCURRENT_BATCHES);
        
        producer.start();
        consumer.start();
        monitor.start();
        
        // 等待完成
        try {
            producer.join();
            consumer.join();
            monitor.interrupt(); // 停止监控
            monitor.join(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("流式迁移被中断", e);
            result.setErrorMessage("迁移被中断");
        }
        
        result.setFinished(true);
        log.info("流式迁移完成 - 处理: {}, 成功: {}, 失败: {}", 
            result.getProcessedCount(), result.getSuccessCount(), result.getFailedCount());
        
        return result;
    }
    
    /**
     * 生产数据 - 读取ES数据并放入队列
     */
    private void produceData(DocumentSupplier supplier, BlockingQueue<SearchHit[]> queue, 
                           int batchSize, AtomicBoolean finished, StreamingMigrationResult result) {
        try {
            SearchHit[] batch;
            long batchCount = 0;
            
            while ((batch = supplier.nextBatch(batchSize)) != null && batch.length > 0) {
                batchCount++;
                
                // 内存压力检查 - 如果内存压力过大，等待消费者处理
                while (getMemoryPressure() > MEMORY_PRESSURE_PAUSE) {
                    log.warn("内存压力过高 {:.1f}%，生产者暂停等待", getMemoryPressure() * 100);
                    Thread.sleep(1000);
                    
                    // 强制GC释放内存
                    System.gc();
                    Thread.sleep(100);
                }
                
                // 放入队列（阻塞式，如果队列满了会等待）
                queue.put(batch);
                
                result.addBatchCount();
                
                if (batchCount % 100 == 0) {
                    log.debug("生产者已生产 {} 批次，队列大小: {}, 内存压力: {:.1f}%", 
                        batchCount, queue.size(), getMemoryPressure() * 100);
                }
                
                // 适应性延迟 - 根据队列大小和内存压力调节
                adaptiveDelay(queue.size(), getMemoryPressure());
            }
            
            log.info("生产者完成，总共生产 {} 批次", batchCount);
        } catch (Exception e) {
            log.error("生产数据异常", e);
            result.setErrorMessage("生产数据异常: " + e.getMessage());
        } finally {
            finished.set(true);
        }
    }
    
    /**
     * 消费数据 - 从队列取数据并写入目标ES
     */
    private void consumeData(RestHighLevelClient targetClient, BlockingQueue<SearchHit[]> queue,
                           DocumentTransformer transformer, MigrationConfig config,
                           AtomicBoolean producerFinished, AtomicBoolean consumerStopped,
                           Semaphore batchSemaphore, StreamingMigrationResult result) {
        try {
            while (!producerFinished.get() || !queue.isEmpty()) {
                SearchHit[] batch = queue.poll(1000, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (batch == null) continue;
                
                // 获取批次处理许可证
                batchSemaphore.acquire();
                
                try {
                    // 异步处理批次，避免阻塞消费者主线程
                    processBatchAsync(targetClient, batch, transformer, config, result, batchSemaphore);
                } catch (Exception e) {
                    log.error("处理批次异常", e);
                    batchSemaphore.release();
                }
            }
            
            // 等待所有批次处理完成
            log.info("等待所有批次处理完成...");
            batchSemaphore.acquire(MAX_CONCURRENT_BATCHES);
            batchSemaphore.release(MAX_CONCURRENT_BATCHES);
            
            log.info("消费者完成");
        } catch (Exception e) {
            log.error("消费数据异常", e);
            result.setErrorMessage("消费数据异常: " + e.getMessage());
        } finally {
            consumerStopped.set(true);
        }
    }
    
    /**
     * 异步处理批次
     */
    private void processBatchAsync(RestHighLevelClient targetClient, SearchHit[] batch,
                                 DocumentTransformer transformer, MigrationConfig config,
                                 StreamingMigrationResult result, Semaphore semaphore) {
        
        // 使用普通线程处理批次
        Thread batchThread = new Thread(() -> {
            try {
                processBatch(targetClient, batch, transformer, config, result);
            } catch (Exception e) {
                log.error("批次处理异常", e);
                result.addFailedCount(batch.length);
            } finally {
                semaphore.release();
            }
        });
        batchThread.start();
    }
    
    /**
     * 处理单个批次
     */
    private void processBatch(RestHighLevelClient targetClient, SearchHit[] batch,
                            DocumentTransformer transformer, MigrationConfig config,
                            StreamingMigrationResult result) throws Exception {
        
        BulkRequest bulkRequest = new BulkRequest();
        int validDocuments = 0;
        
        // 转换文档
        for (SearchHit hit : batch) {
            try {
                Map<String, Object> sourceData = hit.getSourceAsMap();
                Map<String, Object> targetData = transformer.transform(sourceData, config);
                
                IndexRequest request = new IndexRequest(config.getTargetIndex())
                        .id(hit.getId())
                        .source(targetData, XContentType.JSON);
                
                bulkRequest.add(request);
                validDocuments++;
                
            } catch (Exception e) {
                log.warn("文档转换失败，ID: {}, 错误: {}", hit.getId(), e.getMessage());
                result.addFailedCount(1);
            }
        }
        
        // 批量写入
        if (validDocuments > 0) {
            try {
                BulkResponse bulkResponse = targetClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                
                if (bulkResponse.hasFailures()) {
                    // 逐个检查结果
                    long batchSuccess = 0;
                    long batchFailed = 0;
                    
                    for (org.elasticsearch.action.bulk.BulkItemResponse item : bulkResponse.getItems()) {
                        if (item.isFailed()) {
                            batchFailed++;
                            log.debug("文档写入失败: {}, 原因: {}", item.getId(), item.getFailureMessage());
                        } else {
                            batchSuccess++;
                        }
                    }
                    
                    result.addSuccessCount(batchSuccess);
                    result.addFailedCount(batchFailed);
                } else {
                    result.addSuccessCount(validDocuments);
                }
                
                result.addProcessedCount(batch.length);
                
            } catch (Exception e) {
                log.error("批量写入完全失败", e);
                result.addFailedCount(validDocuments);
                result.addProcessedCount(batch.length);
                
                // 如果是网络问题，稍等重试
                if (isNetworkError(e)) {
                    Thread.sleep(5000);
                }
            }
        }
    }
    
    /**
     * 监控内存和流量
     */
    private void monitorMemoryAndFlow(BlockingQueue<SearchHit[]> queue, StreamingMigrationResult result,
                                    AtomicBoolean producerFinished, AtomicBoolean consumerStopped) {
        try {
            while (!producerFinished.get() || !consumerStopped.get()) {
                double memoryPressure = getMemoryPressure();
                int queueSize = queue.size();
                
                log.info("流式监控 - 队列大小: {}, 内存压力: {:.1f}%, 已处理: {}, 成功: {}, 失败: {}", 
                    queueSize, memoryPressure * 100, result.getProcessedCount(), 
                    result.getSuccessCount(), result.getFailedCount());
                
                // 内存压力过高时强制GC
                if (memoryPressure > MEMORY_PRESSURE_THROTTLE) {
                    log.warn("内存压力较高，触发GC");
                    System.gc();
                }
                
                Thread.sleep(10000); // 10秒监控一次
            }
        } catch (InterruptedException e) {
            log.debug("监控线程被中断");
        }
    }
    
    /**
     * 获取内存压力（0.0 - 1.0）
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
     * 适应性延迟
     */
    private void adaptiveDelay(int queueSize, double memoryPressure) throws InterruptedException {
        if (memoryPressure > MEMORY_PRESSURE_THROTTLE || queueSize > QUEUE_CAPACITY * 0.8) {
            // 队列接近满或内存压力大时，生产者稍作延迟
            Thread.sleep((long)(100 * memoryPressure * (queueSize / (double)QUEUE_CAPACITY)));
        }
    }
    
    /**
     * 判断是否为网络错误
     */
    private boolean isNetworkError(Exception e) {
        String message = e.getMessage();
        return message != null && (
            message.contains("timeout") ||
            message.contains("connection") ||
            message.contains("ConnectException") ||
            message.contains("SocketTimeoutException")
        );
    }
    
    /**
     * 文档供应商接口
     */
    public interface DocumentSupplier {
        SearchHit[] nextBatch(int batchSize) throws Exception;
    }
    
    /**
     * 文档转换器接口
     */
    public interface DocumentTransformer {
        Map<String, Object> transform(Map<String, Object> sourceDoc, MigrationConfig config) throws Exception;
    }
    
    /**
     * 流式迁移结果
     */
    public static class StreamingMigrationResult {
        private final AtomicLong processedCount = new AtomicLong(0);
        private final AtomicLong successCount = new AtomicLong(0);
        private final AtomicLong failedCount = new AtomicLong(0);
        private final AtomicLong batchCount = new AtomicLong(0);
        private volatile String errorMessage;
        private volatile boolean finished = false;
        
        public void addProcessedCount(long count) { processedCount.addAndGet(count); }
        public void addSuccessCount(long count) { successCount.addAndGet(count); }
        public void addFailedCount(long count) { failedCount.addAndGet(count); }
        public void addBatchCount() { batchCount.incrementAndGet(); }
        
        public long getProcessedCount() { return processedCount.get(); }
        public long getSuccessCount() { return successCount.get(); }
        public long getFailedCount() { return failedCount.get(); }
        public long getBatchCount() { return batchCount.get(); }
        
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        
        public boolean isFinished() { return finished; }
        public void setFinished(boolean finished) { this.finished = finished; }
    }
}