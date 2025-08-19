package com.everflowx.esmigration.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import lombok.extern.slf4j.Slf4j;
import java.util.Optional;

/**
 * Elasticsearch配置类
 * 支持源ES和目标ES的双连接配置
 * 支持可动态调整的HeapBufferedResponseConsumerFactory
 *
 * @author everflowx
 */
@Slf4j
@Configuration
public class ElasticsearchConfig {

    // 源ES配置
    @Value("${elasticsearch.source.host:localhost}")
    private String sourceHost;
    
    @Value("${elasticsearch.source.port:9200}")
    private int sourcePort;
    
    @Value("${elasticsearch.source.scheme:http}")
    private String sourceScheme;
    
    @Value("${elasticsearch.source.username:}")
    private String sourceUsername;
    
    @Value("${elasticsearch.source.password:}")
    private String sourcePassword;
    
    // 目标ES配置
    @Value("${elasticsearch.target.host:localhost}")
    private String targetHost;
    
    @Value("${elasticsearch.target.port:9200}")
    private int targetPort;
    
    @Value("${elasticsearch.target.scheme:http}")
    private String targetScheme;
    
    @Value("${elasticsearch.target.username:}")
    private String targetUsername;
    
    @Value("${elasticsearch.target.password:}")
    private String targetPassword;

    // 响应缓冲区配置
    @Value("${elasticsearch.response.buffer.limit:524288000}") // 默认500MB
    private int responseBufferLimit;

    @Value("${elasticsearch.response.buffer.initial:10485760}") // 默认10MB
    private int responseBufferInitial;
    
    // 连接池配置
    @Value("${elasticsearch.connection.max-total:200}") // 最大连接数
    private int maxConnectionTotal;
    
    @Value("${elasticsearch.connection.max-per-route:100}") // 每个路由最大连接数
    private int maxConnectionPerRoute;
    
    // 超时配置
    @Value("${elasticsearch.timeout.connect:30000}") // 连接超时，默认30秒
    private int connectTimeout;
    
    @Value("${elasticsearch.timeout.socket:600000}") // Socket超时，默认10分钟
    private int socketTimeout;
    
    @Value("${elasticsearch.timeout.connection-request:60000}") // 连接请求超时，默认1分钟
    private int connectionRequestTimeout;
    
    // 线程池配置
    @Value("${elasticsearch.io.thread-count:0}") // IO线程数，0表示使用CPU核数
    private int ioThreadCount;

    /**
     * 源ES客户端
     */
    @Bean(name = "sourceElasticsearchClient")
    public RestHighLevelClient sourceElasticsearchClient() {
        return createClient(sourceHost, sourcePort, sourceScheme, sourceUsername, sourcePassword);
    }

    /**
     * 目标ES客户端
     */
    @Bean(name = "targetElasticsearchClient")
    @Primary
    public RestHighLevelClient targetElasticsearchClient() {
        return createClient(targetHost, targetPort, targetScheme, targetUsername, targetPassword);
    }

    /**
     * 创建ES客户端，支持可动态调整的响应缓冲区
     */
    private RestHighLevelClient createClient(String host, int port, String scheme, String username, String password) {
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, scheme));

        // 配置HTTP客户端 - 针对百万级数据优化
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            // 设置连接池大小 - 增加并发处理能力
            httpClientBuilder.setMaxConnTotal(maxConnectionTotal);
            httpClientBuilder.setMaxConnPerRoute(maxConnectionPerRoute);
            
            // 设置连接生存时间，避免频繁建连
            httpClientBuilder.setConnectionTimeToLive(5, java.util.concurrent.TimeUnit.MINUTES);

            // 配置IO反应器 - 优化大数据量处理
            int actualIoThreadCount = ioThreadCount > 0 ? ioThreadCount : Runtime.getRuntime().availableProcessors();
            httpClientBuilder.setDefaultIOReactorConfig(
                org.apache.http.impl.nio.reactor.IOReactorConfig.custom()
                    .setIoThreadCount(actualIoThreadCount)
                    .setSoKeepAlive(true)
                    .setTcpNoDelay(true) // 禁用Nagle算法，减少延迟
                    .setSoTimeout(socketTimeout)
                    .setConnectTimeout(connectTimeout)
                    .setSoReuseAddress(true) // 允许地址重用
                    .build()
            );
            
            // 设置连接管理器 - 优化连接复用
            httpClientBuilder.setConnectionManager(
                new org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager(
                    org.apache.http.nio.reactor.IOReactorUtils.create(
                        org.apache.http.impl.nio.reactor.IOReactorConfig.custom()
                            .setIoThreadCount(actualIoThreadCount)
                            .setSoKeepAlive(true)
                            .setTcpNoDelay(true)
                            .build()
                    )
                ) {{
                    setMaxTotal(maxConnectionTotal);
                    setDefaultMaxPerRoute(maxConnectionPerRoute);
                    setValidateAfterInactivity(30000); // 30秒后验证连接
                }}
            );

            // 如果配置了用户名密码，则添加认证
            if (username != null && !username.isEmpty() && password != null && !password.isEmpty()) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }

            return httpClientBuilder;
        });

        // 设置请求配置 - 优化百万级数据迁移超时参数
        builder.setRequestConfigCallback(requestConfigBuilder ->
            requestConfigBuilder
                .setConnectTimeout(connectTimeout)          // 连接超时
                .setSocketTimeout(socketTimeout)            // Socket超时，增加到10分钟
                .setConnectionRequestTimeout(connectionRequestTimeout) // 连接请求超时
                .setExpectContinueEnabled(false)            // 禁用100-Continue，减少延迟
                .setRedirectsEnabled(false)                 // 禁用重定向，避免意外问题
                .setCircularRedirectsAllowed(false)         // 禁用循环重定向
                .setMaxRedirects(0));                       // 最大重定向次数为0

        log.info("创建ES客户端 - Host: {}:{}, 响应缓冲区限制: {}MB, 初始缓冲区: {}MB, " +
                "连接池: {}/{}, IO线程: {}, 超时: {}/{}ms",
                host, port, responseBufferLimit / 1024 / 1024, responseBufferInitial / 1024 / 1024,
                maxConnectionTotal, maxConnectionPerRoute, 
                ioThreadCount > 0 ? ioThreadCount : Runtime.getRuntime().availableProcessors(),
                connectTimeout, socketTimeout);

        return new RestHighLevelClient(builder);
    }

    /**
     * 创建自定义的HeapBufferedResponseConsumerFactory
     * 用于大数据量查询时的内存优化
     */
    @Bean
    public HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory responseConsumerFactory() {
        log.info("配置HeapBufferedResponseConsumerFactory - 缓冲区限制: {}MB",
                responseBufferLimit / 1024 / 1024);
        return new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(responseBufferLimit);
    }

    /**
     * 获取当前的ResponseConsumerFactory实例
     * 支持动态更新缓冲区大小
     */
    public HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory getCurrentResponseConsumerFactory() {
        return new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(responseBufferLimit);
    }

    /**
     * 获取响应缓冲区限制大小
     */
    public int getResponseBufferLimit() {
        return responseBufferLimit;
    }

    /**
     * 获取初始缓冲区大小
     */
    public int getResponseBufferInitial() {
        return responseBufferInitial;
    }

    /**
     * 动态调整响应缓冲区大小
     */
    public void updateResponseBufferLimit(int newLimit) {
        this.responseBufferLimit = newLimit;
        log.info("动态调整响应缓冲区限制为: {}MB", newLimit / 1024 / 1024);
    }

    /**
     * 获取配置了自定义缓冲区的RequestOptions
     */
    public RequestOptions getCustomRequestOptions() {
        try {
            RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
            HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory consumerFactory =
                new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(responseBufferLimit);
            builder.setHttpAsyncResponseConsumerFactory(consumerFactory);

            log.debug("使用自定义缓冲区配置: {}MB", responseBufferLimit / 1024 / 1024);
            return builder.build();
        } catch (Exception e) {
            log.warn("创建自定义RequestOptions失败，使用默认配置: {}", e.getMessage());
            return RequestOptions.DEFAULT;
        }
    }

    /**
     * 获取用于大数据量查询的RequestOptions
     * 使用更大的缓冲区限制
     */
    public RequestOptions getLargeQueryRequestOptions() {
        try {
            RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
            // 对于大查询，使用更大的缓冲区（最大1GB）
            int largeBufferLimit = Math.min(responseBufferLimit * 2, 1073741824); // 1GB
            HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory consumerFactory =
                new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(largeBufferLimit);
            builder.setHttpAsyncResponseConsumerFactory(consumerFactory);

            log.debug("使用大查询缓冲区配置: {}MB", largeBufferLimit / 1024 / 1024);
            return builder.build();
        } catch (Exception e) {
            log.warn("创建大查询RequestOptions失败，使用默认配置: {}", e.getMessage());
            return RequestOptions.DEFAULT;
        }
    }
    
    /**
     * 获取连接池配置信息
     */
    public String getConnectionPoolInfo() {
        return String.format("连接池配置: 总连接数=%d, 每路由连接数=%d, IO线程数=%d", 
            maxConnectionTotal, maxConnectionPerRoute, 
            ioThreadCount > 0 ? ioThreadCount : Runtime.getRuntime().availableProcessors());
    }
    
    /**
     * 获取超时配置信息
     */
    public String getTimeoutInfo() {
        return String.format("超时配置: 连接超时=%ds, Socket超时=%ds, 连接请求超时=%ds", 
            connectTimeout / 1000, socketTimeout / 1000, connectionRequestTimeout / 1000);
    }
}
