# Elasticsearch数据迁移工具

## 项目简介

elasticsearch-data-flow是一个基于Spring Boot开发的Elasticsearch数据迁移和同步工具，支持索引间的高效数据迁移、字段映射、数据转换等功能。

## 核心功能

- **数据迁移**: 支持Elasticsearch索引间的批量数据迁移
- **索引同步**: 实时或定时同步索引数据
- **字段映射**: 支持源索引和目标索引间的字段映射转换
- **数据转换**: 支持新增字段、大小写转换等数据处理
- **批处理**: 可配置批次大小，优化迁移性能
- **多线程**: 支持多线程并发处理，提高迁移效率
- **Web界面**: 提供Swagger API文档界面，便于操作和监控

## 技术栈

- **Java 8**
- **Spring Boot 2.4.2**
- **Elasticsearch 7.10.2**
- **Swagger/Knife4j**: API文档
- **Lombok**: 简化代码
- **Hutool**: 工具类库

## 快速开始

### 环境要求

- JDK 8+
- Maven 3.6+
- Elasticsearch 7.x

### 配置文件

修改 `src/main/resources/application.yml` 配置文件：

```yaml
# Elasticsearch配置
elasticsearch:
  # 源ES配置
  source:
    host: localhost
    port: 9200
    scheme: http
    username: elastic
    password: your_password
  # 目标ES配置
  target:
    host: localhost
    port: 9200
    scheme: http
    username: elastic
    password: your_password

# ES迁移任务配置
es:
  migration:
    task:
      # 是否启用定时任务
      enabled: false
      # 源索引名称
      source-index: source_index
      # 目标索引名称
      target-index: target_index
      # 批次大小
      batch-size: 1000
```

### 运行项目

1. **克隆项目**
   ```bash
   git clone [项目地址]
   cd elasticsearch-data-flow
   ```

2. **编译项目**
   ```bash
   mvn clean compile
   ```

3. **运行项目**
   ```bash
   mvn spring-boot:run
   ```

4. **访问API文档**
   项目启动后会自动打开浏览器访问：http://localhost:6618/doc.html

## API接口

### 数据迁移接口

- **POST** `/api/migration/start` - 启动数据迁移
- **GET** `/api/migration/status` - 查询迁移状态
- **POST** `/api/migration/stop` - 停止迁移任务

### 索引同步接口

- **POST** `/api/sync/start` - 启动索引同步
- **GET** `/api/sync/status` - 查询同步状态
- **POST** `/api/sync/stop` - 停止同步任务

### 配置管理接口

- **GET** `/api/config/migration` - 获取迁移配置
- **POST** `/api/config/migration` - 更新迁移配置

## 使用示例

### 数据迁移配置示例

```json
{
  "sourceIndex": "source_index",
  "targetIndex": "target_index",
  "batchSize": 1000,
  "scrollTimeout": 10,
  "threadCount": 3,
  "overwriteExisting": true,
  "newFields": {
    "migration_time": "now",
    "migration_version": "1.0"
  },
  "caseInsensitiveFields": ["title", "content"],
  "timestampField": "createTime"
}
```

### 字段映射配置

```json
{
  "fieldMapping": {
    "old_field_name": "new_field_name",
    "create_time": "createTime",
    "update_time": "updateTime"
  }
}
```

## 项目结构

```
src/main/java/com/everflowx/esmigration/
├── Application.java              # 启动类
├── config/                       # 配置类
│   ├── ElasticsearchConfig.java  # ES配置
│   └── SwaggerConfig.java        # Swagger配置
├── controller/                   # 控制器
│   ├── EsConfigController.java   # 配置管理
│   ├── EsMigrationController.java # 数据迁移
│   └── IndexSyncController.java  # 索引同步
├── domain/                       # 实体类
│   ├── IndexSyncConfig.java      # 同步配置
│   ├── IndexSyncResult.java      # 同步结果
│   └── MigrationConfig.java      # 迁移配置
├── service/                      # 服务层
│   ├── EsMigrationService.java   # 迁移服务接口
│   ├── IndexSyncService.java     # 同步服务接口
│   └── impl/                     # 服务实现
├── task/                         # 定时任务
│   └── EsMigrationTask.java      # 迁移定时任务
└── util/                         # 工具类
    └── EsQueryHelper.java        # ES查询工具
```

## 注意事项

1. **数据安全**: 迁移前请确保做好数据备份
2. **性能优化**: 根据ES集群性能调整批次大小和线程数
3. **网络稳定**: 确保源ES和目标ES网络连接稳定
4. **权限配置**: 确保ES用户具有相应的读写权限
5. **监控日志**: 关注迁移过程中的错误日志

## 日志配置

项目使用logback进行日志管理，日志文件位于 `log/` 目录：
- `log_info.log`: 信息日志
- `log_warn.log`: 警告日志  
- `log_error.log`: 错误日志

## 开发者

- **作者**: everflowx
- **版本**: 1.0-SNAPSHOT