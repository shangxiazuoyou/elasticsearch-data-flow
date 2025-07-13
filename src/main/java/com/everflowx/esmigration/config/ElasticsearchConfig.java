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
    @Value("${elasticsearch.response.buffer.limit:104857600}") // 默认100MB
    private int responseBufferLimit;

    @Value("${elasticsearch.response.buffer.initial:1048576}") // 默认1MB
    private int responseBufferInitial;

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

        // 配置HTTP客户端
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            // 设置连接池大小
            httpClientBuilder.setMaxConnTotal(100);
            httpClientBuilder.setMaxConnPerRoute(50);

            // 设置默认的ResponseConsumerFactory来处理大响应
            httpClientBuilder.setDefaultIOReactorConfig(
                org.apache.http.impl.nio.reactor.IOReactorConfig.custom()
                    .setIoThreadCount(Runtime.getRuntime().availableProcessors())
                    .setSoKeepAlive(true)
                    .build()
            );

            // 如果配置了用户名密码，则添加认证
            if (username != null && !username.isEmpty() && password != null && !password.isEmpty()) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }

            return httpClientBuilder;
        });

        // 设置请求配置，增加超时时间以处理大数据量
        builder.setRequestConfigCallback(requestConfigBuilder ->
            requestConfigBuilder
                .setConnectTimeout(10000)          // 连接超时10秒
                .setSocketTimeout(300000)          // Socket超时5分钟
                .setConnectionRequestTimeout(30000)); // 连接请求超时30秒

        log.info("创建ES客户端 - Host: {}:{}, 响应缓冲区限制: {}MB, 初始缓冲区: {}KB",
                host, port, responseBufferLimit / 1024 / 1024, responseBufferInitial / 1024);

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
            // 对于大查询，使用更大的缓冲区（最大500MB）
            int largeBufferLimit = Math.min(responseBufferLimit * 2, 524288000); // 500MB
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
}
