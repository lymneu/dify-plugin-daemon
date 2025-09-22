# Dify Plugin Daemon 项目文档

## 1. 项目概述

### 核心功能
Dify Plugin Daemon 是一个用Go语言开发的插件生命周期管理服务，负责：
- 插件的安装、卸载、升级
- 插件调用和状态管理
- 多运行时环境支持（本地、调试、无服务器）
- 集群管理和负载均衡

### 技术栈
- **语言**: Go 1.23+
- **框架**: Gin (HTTP服务)
- **数据库**: MySQL/PostgreSQL
- **缓存**: Redis
- **其他**: Python 3.11+, uv(包管理)

## 2. 核心逻辑

### 2.1 插件生命周期管理

#### 插件状态流转
```
[未安装] → [安装中] → [已安装] → [运行中] → [停止] → [卸载]
```

#### 核心组件
1. **PluginManager** (`internal/core/plugin_manager`)
   - 管理插件实例和生命周期
   - 协调不同运行时环境

2. **SessionManager** (`internal/core/session_manager`)
   - 管理插件调用会话
   - 处理请求/响应流

3. **Cluster** (`internal/cluster`)
   - 集群节点管理
   - 插件调度和负载均衡

### 2.2 插件调用流程

```
HTTP请求 → 中间件验证 → 会话创建 → 插件调用 → 流式响应
```

1. **请求处理**
   - 验证tenant_id和权限
   - 创建Session对象
   - 绑定插件运行时

2. **插件调用**
   - 根据运行时类型选择调用方式
   - 本地：STDIN/STDOUT通信
   - 调试：TCP全双工连接
   - 无服务器：HTTP调用

3. **响应处理**
   - 流式数据传输
   - 错误处理和重试
   - 资源清理

## 3. API接口文档

### 3.1 健康检查
```
GET /health/check
响应: {"status": "ok"}
```

### 3.2 插件管理 API

#### 插件安装
```
POST /plugin/{tenant_id}/management/install/upload/package
Content-Type: multipart/form-data

参数:
- dify_pkg: 插件包文件
- verify_signature: 是否验证签名 (true/false)
```

#### 插件列表
```
GET /plugin/{tenant_id}/management/list
响应: 插件列表
```

#### 插件卸载
```
POST /plugin/{tenant_id}/management/uninstall
参数:
{
  "plugin_unique_identifier": "插件唯一标识"
}
```

### 3.3 插件调用 API

#### 工具调用
```
POST /plugin/{tenant_id}/dispatch/tool/invoke
Header: Authorization: Bearer {server_key}

请求体:
{
  "plugin_unique_identifier": "插件标识",
  "data": {
    "tool": "工具名称",
    "parameters": {参数对象},
    "credentials": {凭证信息}
  }
}
```

#### LLM模型调用
```
POST /plugin/{tenant_id}/dispatch/llm/invoke
请求体:
{
  "plugin_unique_identifier": "插件标识",
  "data": {
    "model": "模型名称",
    "messages": [消息数组],
    "stream": true
  }
}
```

#### 智能体策略调用
```
POST /plugin/{tenant_id}/dispatch/agent_strategy/invoke
请求体:
{
  "plugin_unique_identifier": "插件标识",
  "data": {
    "agent_strategy": "策略名称",
    "query": "查询内容"
  }
}
```

### 3.4 端点管理 API

#### 创建端点
```
POST /plugin/{tenant_id}/endpoint/setup
参数:
{
  "plugin_unique_identifier": "插件标识",
  "name": "端点名称",
  "settings": {配置参数}
}
```

#### 端点调用
```
ALL /e/{hook_id}/*path
支持所有HTTP方法，转发到插件端点
```

### 3.5 OAuth API

#### 获取授权URL
```
POST /plugin/{tenant_id}/dispatch/oauth/get_authorization_url
```

#### 获取凭证
```
POST /plugin/{tenant_id}/dispatch/oauth/get_credentials
```

### 3.6 模型相关 API

#### 文本嵌入
```
POST /plugin/{tenant_id}/dispatch/text_embedding/invoke
```

#### 语音转文本
```
POST /plugin/{tenant_id}/dispatch/speech2text/invoke
```

#### 文本转语音
```
POST /plugin/{tenant_id}/dispatch/tts/invoke
```

## 4. 架构设计

### 4.1 整体架构
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Dify API      │───▶│ Plugin Daemon   │───▶│   Plugin        │
│   Server        │    │                 │    │   Runtime       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │ Redis + Database│
                       │ (状态存储)       │
                       └─────────────────┘
```

### 4.2 核心模块

1. **HTTP服务层** (`internal/server`)
   - Gin路由和中间件
   - 请求验证和响应处理

2. **业务服务层** (`internal/service`)
   - 插件安装和管理
   - 调用逻辑封装

3. **插件管理器** (`internal/core/plugin_manager`)
   - 插件实例管理
   - 运行时协调

4. **会话管理** (`internal/core/session_manager`)
   - 会话状态管理
   - 并发安全

5. **集群管理** (`internal/cluster`)
   - 节点发现和健康检查
   - 请求路由和负载均衡

### 4.3 数据流

1. **请求流**：HTTP → 中间件 → 服务层 → 插件管理器 → 运行时
2. **响应流**：运行时 → 会话管理 → 流式传输 → HTTP响应
3. **状态同步**：本地状态 ←→ Redis ←→ 其他节点

## 5. 部署和配置

### 5.1 环境配置
```bash
# 构建
go build -o daemon cmd/server/main.go

# 运行
./daemon
```

### 5.2 关键配置项
- `SERVER_PORT`: HTTP服务端口
- `SERVER_KEY`: API访问密钥
- `REDIS_HOST`: Redis服务地址
- `DB_TYPE`: 数据库类型(mysql/postgresql)
- `PLATFORM`: 运行平台(local/serverless)

## 6. 限制和注意事项

### 6.1 水平扩展限制
- **社区版不支持水平扩展**
- 插件状态绑定到特定节点
- 会话管理使用本地内存

### 6.2 性能考虑
- 插件调用为异步流式处理
- Redis用于集群状态同步
- 支持并发插件调用

### 6.3 安全要求
- 插件包签名验证
- API密钥认证
- 权限检查机制

## 7. 开发指南

### 7.1 添加新的插件类型
1. 在`pkg/entities`中定义实体
2. 在`internal/core/plugin_daemon`中实现调用逻辑
3. 使用代码生成器生成API控制器

### 7.2 扩展运行时
1. 实现`PluginLifetime`接口
2. 在`PluginManager`中注册新运行时
3. 更新配置和部署脚本

这份文档概括了Dify Plugin Daemon的核心架构、API接口和重要技术细节，为开发和运维提供参考。