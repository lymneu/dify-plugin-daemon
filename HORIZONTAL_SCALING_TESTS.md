# Dify Plugin Daemon 水平扩展测试指南

## 概述

本文档描述了如何运行dify-plugin-daemon水平扩展功能的测试用例，验证分布式架构的正确性。

## 测试环境要求

### 必需组件
- Go 1.23+
- Redis 服务器（用于分布式状态存储）
- 测试依赖包：
  - `github.com/stretchr/testify`

### Redis 配置
测试需要运行中的Redis服务器，默认配置：
- 主机：localhost
- 端口：6379
- 数据库：
  - DB 1：会话管理测试
  - DB 2：集群管理测试

## 测试分类

### 1. 分布式会话管理测试
文件：`internal/core/session_manager/distributed_session_test.go`

```bash
# 运行会话管理测试
go test ./internal/core/session_manager -v -run TestDistributedSession
```

**测试内容：**
- 分布式会话存储和检索
- 会话过期机制
- 本地缓存功能
- 跨节点会话同步

### 2. 分布式插件状态管理测试
文件：`internal/cluster/distributed_plugin_state_test.go`

```bash
# 运行插件状态管理测试
go test ./internal/cluster -v -run TestDistributedPluginStateManager
```

**测试内容：**
- 插件注册和注销
- 插件状态更新
- 跨节点插件发现
- 本地缓存机制
- TTL过期处理
- 并发访问安全

### 3. 负载均衡器测试
文件：`internal/cluster/load_balancer_test.go`

```bash
# 运行负载均衡测试
go test ./internal/cluster -v -run TestDistributedLoadBalancer
```

**测试内容：**
- 轮询（Round Robin）策略
- 最少连接（Least Connections）策略
- 随机（Random）策略
- 加权随机（Weighted Random）策略
- 节点排除机制
- 健康检查

### 4. 集成测试
文件：`internal/cluster/horizontal_scaling_integration_test.go`

```bash
# 运行集成测试
go test ./internal/cluster -v -run TestHorizontalScaling
```

**测试内容：**
- 多节点插件分布
- 故障转移场景
- 并发请求处理
- 动态扩缩容

## 运行所有测试

```bash
# 运行所有水平扩展相关测试
go test ./internal/core/session_manager ./internal/cluster -v

# 仅运行分布式相关测试
go test ./internal/core/session_manager ./internal/cluster -v -run "Distributed|HorizontalScaling"
```

## 测试数据清理

测试会自动清理Redis中的测试数据，但如需手动清理：

```bash
# 连接Redis
redis-cli

# 清理测试数据库
SELECT 1
FLUSHDB
SELECT 2 
FLUSHDB
```

## 性能基准测试

```bash
# 运行性能基准测试
go test ./internal/cluster -bench=BenchmarkDistributedLoadBalancer -benchmem

# 并发性能测试
go test ./internal/cluster -bench=BenchmarkConcurrentPluginSelection -benchmem
```

## 故障排除

### Redis连接失败
如果测试跳过并显示"Redis not available"：
1. 确认Redis服务正在运行
2. 检查连接配置（localhost:6379）
3. 确认Redis允许本地连接

### 测试超时
如果测试运行缓慢或超时：
1. 检查Redis性能
2. 调整测试并发数
3. 增加测试超时时间：`go test -timeout 60s`

### 并发测试失败
如果并发测试不稳定：
1. 检查Redis配置的最大连接数
2. 调整测试中的并发goroutine数量
3. 确保系统资源充足

## 配置验证

运行配置验证测试：

```bash
go test ./internal/types/app -v -run TestHorizontalScalingConfig
```

## 测试覆盖率

生成测试覆盖率报告：

```bash
# 生成覆盖率报告
go test ./internal/cluster ./internal/core/session_manager -coverprofile=coverage.out

# 查看覆盖率
go tool cover -html=coverage.out
```

## 集成到CI/CD

在CI/CD流水线中运行测试：

```yaml
# GitHub Actions 示例
- name: Start Redis
  uses: supercharge/redis-github-action@1.4.0
  with:
    redis-version: 7

- name: Run Horizontal Scaling Tests  
  run: |
    go test ./internal/core/session_manager ./internal/cluster -v -race
```

## 生产环境注意事项

在生产环境启用水平扩展前，请确保：

1. **Redis集群配置**：推荐使用Redis集群或高可用配置
2. **网络延迟**：节点间网络延迟应控制在合理范围内
3. **监控**：配置负载均衡和插件状态监控
4. **故障恢复**：制定节点故障的恢复策略

## 相关文档

- [项目文档](./PROJECT_DOCUMENTATION.md)
- [水平扩展配置](./.env.horizontal-scaling.example)
- [负载均衡策略](./internal/types/app/horizontal_scaling_config.go)