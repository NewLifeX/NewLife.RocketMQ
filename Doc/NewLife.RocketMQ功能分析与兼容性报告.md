# NewLife.RocketMQ 功能分析与兼容性报告

> 文档生成时间：2026年2月（更新于2026年7月）  
> 当前客户端版本：2.9.x  
> 协议版本声明：`MQVersion.V4_9_7`  
> 目标框架：net45 / net461 / netstandard2.0 / netstandard2.1

---

## 一、项目概述

NewLife.RocketMQ 是新生命团队开发的**纯托管轻量级 RocketMQ 客户端**，基于 RocketMQ Remoting 协议（TCP 私有协议）实现，同时支持 **gRPC Proxy 协议**（RocketMQ 5.x），不依赖 Java 客户端或第三方 gRPC 库。

### 核心架构

```
MqBase (业务基类)
├── Producer (生产者)
└── Consumer (消费者)

ClusterClient (集群客户端/通信层，Remoting协议)
├── NameClient (名称服务器客户端)
└── BrokerClient (Broker客户端)

Grpc/ (gRPC传输层，RocketMQ 5.x Proxy协议，netstandard2.1+)
├── GrpcClient (HTTP/2 gRPC客户端，帧编解码)
├── GrpcMessagingService (消息服务：发送/接收/确认/路由/心跳)
├── ProtoWriter / ProtoReader (轻量级Protobuf编解码器)
├── GrpcModels (Resource/Endpoints/Message/SystemProperties等)
├── GrpcServiceMessages (Request/Response消息类型)
└── GrpcEnums (GrpcCode/GrpcMessageType等枚举)

Protocol/
├── Command (命令帧，Remoting协议编解码)
├── MqCodec (网络编解码器)
├── Header (通信头)
├── Message / MessageExt (消息模型)
├── SendMessageRequestHeader / PullMessageRequestHeader
├── EndTransactionRequestHeader
└── RequestCode / ResponseCode (指令码)
```

### 通信层特点
- 基于 `NewLife.Net` 的 TCP 长连接（Remoting 协议）
- 基于 `HttpClient` HTTP/2 的 gRPC 协议（RocketMQ 5.x Proxy，netstandard2.1+）
- 支持 JSON 和 RocketMQ 二进制两种序列化格式
- 内置轻量级 Protobuf 编解码器（无外部依赖）
- 支持 SSL/TLS 加密传输
- 支持多云厂商签名认证（阿里云/华为云/腾讯云/Apache ACL），统一由 `ICloudProvider` 适配
- 单连接复用，Opaque 请求-响应匹配

---

## 二、RocketMQ 各主要版本功能矩阵

### 2.1 RocketMQ 4.x（4.0 ~ 4.9）— 经典 Remoting 协议

| 功能 | 版本引入 | NewLife 支持 | 备注 |
|------|---------|:----------:|------|
| **普通消息发送（同步）** | 4.0 | ? | `Publish()` / `SEND_MESSAGE_V2` |
| **普通消息发送（异步）** | 4.0 | ? | `PublishAsync()` |
| **单向发送（Oneway）** | 4.0 | ? | `PublishOneway()` |
| **Pull 模式消费** | 4.0 | ? | `Pull()` / `PULL_MESSAGE` |
| **集群消费模式** | 4.0 | ? | `MessageModels.Clustering` |
| **广播消费模式** | 4.0 | ? | `MessageModels.Broadcasting` |
| **消费者负载均衡（平均分配）** | 4.0 | ? | `Rebalance()` 平均分配算法 |
| **Tag 过滤** | 4.0 | ? | `Tags` / `Subscription` |
| **延迟消息（18级定时）** | 4.0 | ? | `PublishDelay()` / `DelayTimeLevels` |
| **事务消息（半消息）** | 4.3 | ? | `PublishTransaction()` / `EndTransaction()` |
| **顺序消息** | 4.0 | ? | `Publish(message, queue)` 指定队列 |
| **消息轨迹** | 4.4 | ? | `AsyncTraceDispatcher` / `MessageTraceHook` |
| **消费者偏移管理** | 4.0 | ? | `QueryOffset` / `UpdateOffset` / `QueryMaxOffset` / `QueryMinOffset` |
| **心跳机制** | 4.0 | ? | `BrokerClient.Ping()` |
| **Topic 创建/更新** | 4.0 | ? | `CreateTopic()` |
| **消费组信息查询** | 4.0 | ? | `GetConsumers()` |
| **Request-Reply 模式** | 4.6 | ? | `Request()` / `RequestAsync()` |
| **ACL 权限控制** | 4.4 | ? | `AclOptions` HMAC-SHA1 签名 |
| **SQL92 过滤** | 4.1 | ? | `ExpressionType=SQL92` + Subscription 填写SQL表达式 |
| **批量消息发送** | 4.5 | ? | `PublishBatch()` / `SEND_BATCH_MESSAGE (320)` |
| **消息压缩（发送端）** | 4.0 | ? | `CompressOverBytes` 超过阈值自动ZLIB压缩 + SysFlag标记 |
| **消息回退（消费失败）** | 4.0 | ? | `SendMessageBack()` / `CONSUMER_SEND_MSG_BACK (36)` |
| **事务回查（被动回查）** | 4.3 | ? | `OnCheckTransaction` / `CHECK_TRANSACTION_STATE (39)` 回调处理 |
| **按时间戳搜索偏移** | 4.0 | ? | `SearchOffset()` / `SEARCH_OFFSET_BY_TIMESTAMP` |
| **消费进度持久化（本地）** | 4.0 | ? | `OffsetStorePath` 广播模式本地JSON文件持久化，集群模式仍走Broker端存储 |
| **Push 模式（服务端推送）** | 4.0 | ?? | 本质是长轮询 Pull 模拟，已实现但非原生 Push |
| **Pop 消费模式** | 4.9.3 | ? | `PopMessageAsync()` / `AckMessageAsync()` / `ChangeInvisibleTimeAsync()` |
| **多 Topic 订阅** | 4.0 | ? | `Topics` 属性支持多主题订阅，Rebalance 按 Topic 分别分配队列 |
| **消费重试** | 4.0 | ? | `EnableRetry` + `MaxReconsumeTimes`，消费失败自动回退到RETRY Topic |
| **死信队列（DLQ）** | 4.0 | ? | 超过最大重试次数后自动进入 `%DLQ%{ConsumerGroup}` 主题 |
| **消费限流/线程池控制** | 4.0 | ? | `MaxConcurrentConsume` 信号量控制所有队列的总并发 |
| **VIP 通道** | 4.0 | ? | 代码已注释 `VipChannelEnabled` |
| **Broker 主从切换** | 4.5 | ? | `BrokerInfo.MasterAddress`/`SlaveAddresses`，消费失败自动切换从节点读取 |

### 2.2 RocketMQ 5.x（5.0+）— 新架构

RocketMQ 5.0 引入了全新的 **gRPC Proxy** 架构，同时保持对 Remoting 协议的向后兼容。

| 功能 | 版本引入 | NewLife 支持 | 备注 |
|------|---------|:----------:|------|
| **gRPC Proxy 协议** | 5.0 | ? | 内置轻量 Protobuf 编解码 + HTTP/2 gRPC 客户端，通过 `GrpcProxyAddress` 属性启用（netstandard2.1+） |
| **Remoting 协议兼容** | 5.0 | ? | 5.x Broker 保留了 Remoting 接口，当前客户端可连接 |
| **任意时间延迟消息** | 5.0 | ? | `PublishDelayViaGrpcAsync()` 通过 gRPC 协议支持任意时间戳延迟消息 |
| **Pop 消费模式** | 5.0 | ? | `PopMessageAsync()` / `AckMessageAsync()` / `ChangeInvisibleTimeAsync()` |
| **客户端主动上报资源** | 5.0 | ? | gRPC 协议特有，Remoting 兼容模式不需要 |
| **消息分组（FIFO）** | 5.0 | ?? | Remoting 模式下等价于顺序消息 |
| **服务端 Rebalance** | 5.0 | ? | 需 Broker 5.0 + gRPC 协议配合 |
| **MessageId 新格式** | 5.0 | ? | 5.x 引入新的 MessageId 编码规则 |
| **Controller 模式** | 5.0 | ?? | 替代 DLedger 的高可用方案，客户端无感知 |
| **Compaction Topic** | 5.1 | ? | KV 语义 Topic |
| **Timer 消息（5.0 原生）** | 5.0 | ? | 通过 `TIMER_DELIVER_MS` 属性实现 |

### 2.3 版本兼容性总结

| 服务端版本 | 连接能力 | 核心功能 | 已知问题 |
|-----------|:------:|:------:|---------|
| **4.0 ~ 4.3** | ? | ? | 无事务消息支持 |
| **4.4 ~ 4.9** | ? | ? | 完全兼容，主力测试版本 |
| **5.0 ~ 5.x（Remoting模式）** | ? | ? | 新特性（Pop/Timer）不可用 |
| **5.0 ~ 5.x（gRPC Proxy模式）** | ? | ? | 通过 `GrpcProxyAddress` 属性启用，需 netstandard2.1+ |

---

## 三、阿里云 RocketMQ 专有版本兼容性

### 3.1 阿里云 RocketMQ 4.x 实例

| 功能 | 支持状态 | 实现方式 |
|------|:------:|---------|
| **实例ID路由** | ? | `{InstanceId}%{Topic}` / `{InstanceId}%{Group}` 前缀拼接 |
| **名称服务器发现** | ? | HTTP 接口 `onsaddr-internet.aliyun.com` 获取 NameServer 地址 |
| **AccessKey/SecretKey 签名** | ? | `AliyunOptions` + HMAC-SHA1 签名 |
| **OnsChannel 标识** | ? | 默认 `ALIYUN` |
| **实例ID自动解析** | ? | 从 NameServer 地址中提取 `MQ_INST_` 前缀 |
| **公网版消费者状态** | ?? | 阿里云公网版返回的 `brokerName` 与 NameServer 路由中的不一致，需特殊处理（已有注释说明） |
| **消费者状态JSON** | ? | `ConsumerStatesSpecialJsonHandler` 处理阿里云特殊JSON格式 |
| **铂金版/独享实例** | ?? | 理论兼容，但未有充分测试 |
| **多 Tag 过滤** | ? | 通过 `Tags` 属性支持 |

### 3.2 阿里云 RocketMQ 5.x 实例

| 功能 | 支持状态 | 备注 |
|------|:------:|------|
| **Remoting 协议兼容** | ?? | 阿里云 5.x 实例保留 Remoting 接口，理论上可连接 |
| **gRPC Proxy 模式** | ? | 阿里云推荐使用 gRPC 方式接入，当前不支持 |
| **实例级别认证** | ?? | 5.x 可能使用新的认证体系 |
| **Serverless 实例** | ? | 仅支持 gRPC 接入 |

### 3.3 当前阿里云适配的已知问题

1. **公网版 BrokerName 不匹配**：阿里云公网版 RocketMQ 返回的消费者状态中 `brokerName` 是真实 Broker 名称，而 NameServer 路由返回的是网关名称，导致 `InitOffsetAsync` 中偏移匹配失败。当前代码用 `?? new OffsetWrapperModel()` 容错处理。
2. **实例ID自动识别限制**：仅支持从 NameServer 地址中提取 `MQ_INST_` 前缀，无法处理自定义实例ID。

---

## 四、华为云 RocketMQ 专有版本兼容性

### 4.1 华为云 DMS for RocketMQ

华为云的分布式消息服务（DMS）提供 RocketMQ 兼容实例。

| 功能 | 支持状态 | 备注 |
|------|:------:|------|
| **标准 Remoting 协议** | ?? | 华为云 DMS 4.x 兼容实例理论上可连接，需要验证 |
| **SSL/TLS 加密** | ? | 客户端支持 `SslProtocol` 和 `Certificate` 配置 |
| **SASL 认证** | ? | 华为云可能使用 SASL 认证，当前不支持 |
| **ACL 认证** | ?? | 华为云 DMS 使用自己的 AccessKey 体系，需验证是否与 Apache ACL 兼容 |
| **华为云 5.x 实例** | ? | 可能仅支持 gRPC Proxy |

### 4.2 华为云适配

当前客户端已增加华为云基础适配代码：
- `HuaweiOptions.cs` — 华为云 DMS 连接参数（AccessKey/SecretKey/InstanceId）
- `MqBase.Huawei` — 华为云配置属性
- `ClusterClient.SetSignature()` — 支持华为云签名（复用 ACL 签名机制）

**使用方式**（`ICloudProvider`）：
```csharp
var producer = new Producer
{
    Topic = "test",
    NameServerAddress = "华为云DMS实例地址:9876",
    CloudProvider = new HuaweiProvider
    {
        AccessKey = "你的AK",
        SecretKey = "你的SK",
        InstanceId = "实例ID",
        EnableSsl = true,
    }
};
```

> 旧版 `MqBase.Huawei` 属性和 `HuaweiOptions` 类已废弃并移除，请统一使用 `CloudProvider = new HuaweiProvider()`。

**待验证事项**：
1. 华为云 DMS 的 ACL 签名是否与 Apache ACL 完全一致
2. SSL/TLS 证书验证是否需要特殊处理
3. NameServer 地址发现机制

---

## 五、腾讯云 TDMQ RocketMQ 版兼容性

### 5.1 腾讯云 TDMQ for RocketMQ

腾讯云 TDMQ（Tencent Distributed Message Queue）提供 RocketMQ 兼容版本，基于 Apache RocketMQ 4.x 协议。

| 功能 | 支持状态 | 备注 |
|------|:------:|------|
| **标准 Remoting 协议** | ?? | 腾讯云 TDMQ 4.x 兼容实例，理论上可通过 Remoting 协议连接 |
| **SSL/TLS 加密** | ? | 客户端支持 `SslProtocol` 和 `Certificate` 配置 |
| **HMAC-SHA1 签名认证** | ?? | 腾讯云使用类似 HMAC-SHA1 的签名方式，需验证与 Apache ACL 兼容性 |
| **ACL 认证** | ?? | 可尝试通过 `AclOptions` 配置 AccessKey/SecretKey |
| **VPC 内网访问** | ? | 直接配置 NameServer 为 VPC 内网地址即可 |
| **公网访问** | ?? | 可能需要额外的认证或路由配置 |
| **Topic/Group 管理** | ?? | 腾讯云通常通过控制台管理，API 兼容性待验证 |
| **消息轨迹** | ?? | 腾讯云有自己的消息轨迹系统，客户端轨迹可能不兼容 |
| **延迟消息** | ? | 标准 18 级延迟消息，与 Apache 4.x 一致 |
| **事务消息** | ?? | 需验证腾讯云对半消息和回查的处理 |

### 5.2 腾讯云适配

当前客户端已提供腾讯云专用适配器 `TencentProvider`，支持 Namespace 前缀路由：

**使用方式**：
```csharp
var producer = new Producer
{
    Topic = "test",
    NameServerAddress = "腾讯云TDMQ实例地址:9876",
    CloudProvider = new TencentProvider
    {
        AccessKey = "腾讯云SecretId",
        SecretKey = "腾讯云SecretKey",
        Namespace = "命名空间",
    }
};
```

**待验证事项**：
1. 验证 HMAC-SHA1 签名是否与腾讯云 TDMQ 兼容
2. 确认 Namespace 路由规则是否与 `{Namespace}%{Topic}` 一致
3. 腾讯云 TDMQ 5.x 版本的 Remoting 兼容性

---

## 五、已实现协议详细分析

### 5.1 已实现的 RequestCode（服务端请求码）

| RequestCode | 值 | 用途 | 调用位置 |
|-------------|:--:|------|---------|
| `SEND_MESSAGE` | 10 | 发消息（V1，已不使用） | 仅定义 |
| `PULL_MESSAGE` | 11 | 拉取消息 | `Consumer.Pull()` |
| `QUERY_CONSUMER_OFFSET` | 14 | 查询消费偏移 | `Consumer.QueryOffset()` |
| `UPDATE_CONSUMER_OFFSET` | 15 | 更新消费偏移 | `Consumer.UpdateOffset()` |
| `UPDATE_AND_CREATE_TOPIC` | 17 | 创建/更新Topic | `MqBase.CreateTopic()` |
| `GET_BROKER_RUNTIME_INFO` | 28 | Broker运行信息 | `BrokerClient.GetRuntimeInfo()` |
| `GET_MAX_OFFSET` | 30 | 最大偏移量 | `Consumer.QueryMaxOffset()` |
| `GET_MIN_OFFSET` | 31 | 最小偏移量 | `Consumer.QueryMinOffset()` |
| `HEART_BEAT` | 34 | 心跳 | `BrokerClient.Ping()` |
| `UNREGISTER_CLIENT` | 35 | 注销客户端 | `BrokerClient.UnRegisterClient()` |
| `END_TRANSACTION` | 37 | 结束事务 | `Producer.EndTransaction()` |
| `GET_CONSUMER_LIST_BY_GROUP` | 38 | 消费者列表 | `Consumer.GetConsumers()` |
| `GET_ROUTEINTO_BY_TOPIC` | 105 | Topic路由 | `NameClient.GetRouteInfo()` |
| `GET_CONSUME_STATS` | 208 | 消费状态 | `Consumer.InitOffsetAsync()` |
| `SEND_MESSAGE_V2` | 310 | 发消息V2 | `Producer.Publish()` / `PublishAsync()` |

### 5.2 已定义但未实现（调用）的 RequestCode

| RequestCode | 值 | 用途 | 重要性 |
|-------------|:--:|------|:------:|
| `QUERY_MESSAGE` | 12 | 查询消息 | ?已实现 `QueryMessageByKey()` |
| `QUERY_BROKER_OFFSET` | 13 | 查询Broker偏移 | 低 |
| `GET_ALL_TOPIC_CONFIG` | 21 | 所有Topic配置 | 低 |
| `SEARCH_OFFSET_BY_TIMESTAMP` | 29 | 按时间戳搜索偏移 | ?已实现 |
| `GET_EARLIEST_MSG_STORETIME` | 32 | 最早消息存储时间 | 中 |
| `VIEW_MESSAGE_BY_ID` | 33 | 按ID查看消息 | ?已实现 |
| `CONSUMER_SEND_MSG_BACK` | 36 | 消费失败回退 | ?已实现 |
| `CHECK_TRANSACTION_STATE` | 39 | 事务回查 | ?已实现 |
| `NOTIFY_CONSUMER_IDS_CHANGED` | 40 | 消费者变更通知 | ?已处理 |
| `LOCK_BATCH_MQ` | 41 | 锁定队列（顺序消费） | ?已实现 `LockBatchMQAsync()` |
| `UNLOCK_BATCH_MQ` | 42 | 解锁队列 | ?已实现 `UnlockBatchMQAsync()` |
| `GET_BROKER_CLUSTER_INFO` | 106 | 集群信息 | ?已实现 `GetClusterInfo()` |
| `GET_CONSUMER_RUNNING_INFO` | 307 | 消费者运行信息 | ?已处理 |
| `SEND_BATCH_MESSAGE` | 320 | 批量发送 | ?已实现 |
| `REQUEST_MESSAGE` | 323 | Request-Reply | 已用其他方式实现 |
| `SEND_REPLY_MESSAGE_V2` | 325 | 回复消息 | 中 |
| `POP_MESSAGE` | 200050 | Pop消费 | ?已实现 `PopMessageAsync()` |
| `ACK_MESSAGE` | 200051 | Pop消息确认 | ?已实现 `AckMessageAsync()` |
| `CHANGE_MESSAGE_INVISIBLETIME` | 200052 | 修改不可见时间 | ?已实现 `ChangeInvisibleTimeAsync()` |
| `BATCH_ACK_MESSAGE` | 200151 | 批量Pop确认 | ?已定义 |

### 5.3 消息编解码分析

**消息发送（MessageExt.Write）**：仅返回 `true`，不实际写入，因为发送走的是 `Command` 封装。

**消息接收（MessageExt.Read）**：
- ? 支持标准的 4.x 消息二进制格式
- ? 支持 ZLIB 解压缩（SysFlag 第0位）
- ? 支持 IPv4 地址解析
- ? 支持 IPv6 地址解析（SysFlag 第2位标识，自动适配4字节/16字节IP）
- ? **支持批量消息解码**（`ReadBatch()` 方法解码 BatchMessage Body 内嵌多条消息）

### 5.4 序列化格式

| 格式 | 支持 | 说明 |
|------|:---:|------|
| JSON | ? | 默认格式，使用 `NewLife.Serialization.JsonHelper` |
| ROCKETMQ（二进制） | ? | 命令头部支持二进制解析 |

---

## 六、功能完善度评估

### 6.1 生产者功能（Producer）

| 功能 | 状态 | 详细说明 |
|------|:----:|---------|
| 同步发送 | ?完整 | 支持重试、超时、负载均衡 |
| 异步发送 | ?完整 | 异步版本，支持 CancellationToken |
| 单向发送 | ?完整 | 不等待结果 |
| 延迟消息 | ?完整 | 18级定时消息 |
| 事务消息 | ?完整 | 支持发送半消息、提交/回滚，支持被动回查回调 `OnCheckTransaction` |
| 顺序消息 | ?完整 | 通过指定 `MessageQueue` 参数实现 |
| Request-Reply | ?完整 | 同步/异步请求响应模式 |
| 批量发送 | ?完整 | `PublishBatch()` 支持批量消息发送，`SEND_BATCH_MESSAGE` |
| 消息压缩 | ?完整 | 发送端超过 `CompressOverBytes` 阈值自动ZLIB压缩 |
| 发送端钩子 | ?完整 | `ISendMessageHook` 前后拦截 |
| 消息轨迹 | ?完整 | 异步轨迹分发器 |
| 任意时间延迟 | ?完整 | gRPC 模式下 `PublishDelayViaGrpcAsync()` 支持任意时间戳延迟 |

### 6.2 消费者功能（Consumer）

| 功能 | 状态 | 详细说明 |
|------|:----:|---------|
| Pull 模式 | ?完整 | 长轮询拉取 |
| 消费调度 | ?完整 | 自动分配队列并启动消费线程 |
| 集群消费 | ?完整 | 平均分配 Rebalance 算法 |
| 广播消费 | ?完整 | `OffsetStorePath` 本地JSON文件持久化偏移 |
| Tag 过滤 | ?完整 | 支持 Tag 表达式 |
| 偏移管理 | ?完整 | 查询/更新/持久化 |
| 消费者信息上报 | ?完整 | `GetConsumerRunningInfo` |
| 消费者变更通知 | ?完整 | `NOTIFY_CONSUMER_IDS_CHANGED` 触发重平衡 |
| 消费重试 | ?完整 | `EnableRetry` + `MaxReconsumeTimes`，自动回退到RETRY Topic |
| 死信队列 | ?完整 | 超过最大重试次数后自动进入 `%DLQ%` Topic |
| 消费回退 | ?完整 | `SendMessageBack()` / `CONSUMER_SEND_MSG_BACK` |
| SQL92 过滤 | ?完整 | `ExpressionType="SQL92"` + SQL表达式 |
| 顺序消费（锁定） | ?完整 | `LockBatchMQAsync()` / `UnlockBatchMQAsync()` / `OrderConsume` 属性 |
| 多Topic订阅 | ?完整 | `Topics` 属性支持多主题订阅，Rebalance 按 Topic 分别获取 Broker 构建队列 |
| Pop 消费 | ?完整 | `PopMessageAsync()` / `AckMessageAsync()` / `ChangeInvisibleTimeAsync()` |
| 按时间戳消费 | ?完整 | `SearchOffset()` / `SEARCH_OFFSET_BY_TIMESTAMP` |
| 消费限流 | ?完整 | `MaxConcurrentConsume` 信号量控制总并发 |
| 消费端钩子 | ?完整 | `IConsumeMessageHook` 前后拦截 |

### 6.3 管理功能

| 功能 | 状态 | 详细说明 |
|------|:----:|---------|
| Topic 创建/更新 | ?完整 | 在所有 Broker 上创建 |
| Broker 运行信息 | ?完整 | `GetRuntimeInfo()` |
| 消费组查询 | ?完整 | `GetConsumers()` |
| 消费统计 | ??部分 | `GET_CONSUME_STATS` 仅用于初始化偏移 |
| Topic 删除 | ?完整 | `DeleteTopic()` / `DELETE_TOPIC_IN_BROKER` + `DELETE_TOPIC_IN_NAMESRV` |
| 消费组创建/删除 | ?完整 | `CreateSubscriptionGroup()` / `DeleteSubscriptionGroup()` |
| 消息查询（按ID） | ?完整 | `ViewMessage()` / `VIEW_MESSAGE_BY_ID` |
| 消息查询（按Key） | ?完整 | `QueryMessageByKey()` / `QUERY_MESSAGE` |
| 集群信息查询 | ?完整 | `GetClusterInfo()` / `GET_BROKER_CLUSTER_INFO` |
| 消费者连接列表 | ?完整 | `GetConsumerConnectionList()` / `GET_CONSUMER_CONNECTION_LIST` |
| 偏移重置 | ?完整 | `ResetConsumerOffset()` / `INVOKE_BROKER_TO_RESET_OFFSET` |

---

## 七、各家专有版本对比

### 7.1 功能支持对比表

| 功能 | Apache 4.x | Apache 5.x | 阿里云 4.x | 阿里云 5.x | 华为云 DMS | 腾讯云 TDMQ |
|------|:----------:|:----------:|:---------:|:---------:|:---------:|:---------:|
| Remoting连接 | ? | ? | ? | ?? | ?? | ?? |
| gRPC连接 | N/A | ? | N/A | ?? | ?? | ?? |
| 签名认证 | ?ACL | ?ACL | ?阿里签名 | ?? | ?华为云适配 | ??待验证 |
| 实例ID路由 | N/A | N/A | ? | ? | 待验证 | 待验证 |
| NameServer发现 | 直连 | 直连 | ?HTTP | 待验证 | 待验证 | VPC直连 |
| SSL/TLS | ? | ? | ? | ? | ? | ? |

### 7.2 专有版本适配代码清单

**阿里云适配**（已有）：
- `AliyunOptions.cs` — 阿里云连接参数
- `MqBase.Start()` — 实例ID前缀拼接
- `MqBase.OnStart()` — HTTP获取NameServer地址
- `ClusterClient.SetSignature()` — HMAC-SHA1签名
- `Consumer.ConsumerStatesSpecialJsonHandler()` — 特殊JSON解析

**Apache ACL 适配**（已有）：
- `AclOptions.cs` — ACL参数
- `ClusterClient.SetSignature()` — 复用签名逻辑

**华为云适配**（已有）：
- `HuaweiProvider.cs` — 华为云适配器（`ICloudProvider` 实现，替代旧版 `HuaweiOptions`）
- `ClusterClient.SetSignature()` — 华为云签名（复用 ACL 签名逻辑）

**腾讯云适配**（已有）：
- `TencentProvider.cs` — 腾讯云 TDMQ 适配器（`ICloudProvider` 实现）
- 支持 Namespace 前缀路由
- `ClusterClient.SetSignature()` — 复用签名逻辑

**云厂商适配器接口**（新增）：
- `ICloudProvider.cs` — 统一云厂商适配接口
- `AliyunProvider.cs` — 阿里云适配器（替代旧版 `AliyunOptions`）
- `AclProvider.cs` — Apache ACL 适配器（替代旧版 `AclOptions`）
- `HuaweiProvider.cs` — 华为云适配器（替代旧版 `HuaweiOptions`）
- `TencentProvider.cs` — 腾讯云适配器（新增）

---

## 八、与官方 Java 客户端功能差距

以下是与 Apache RocketMQ 官方 Java 客户端 4.9.x 对比的主要差距：

### 8.1 高优先级缺失功能

| 功能 | 重要性 | 实现难度 | 说明 |
|------|:------:|:------:|------|
| **消费重试机制** | ?已实现 | 中 | `EnableRetry` + `MaxReconsumeTimes`，消费失败后自动发送到 `%RETRY%{ConsumerGroup}` Topic |
| **事务回查回调** | ?已实现 | 中 | Broker 主动调用 `CHECK_TRANSACTION_STATE`，客户端通过 `OnCheckTransaction` 回应事务状态 |
| **批量消息发送** | ?已实现 | 低 | `PublishBatch()`，将多条消息合并为一个请求发送 |
| **消费回退（SendBack）** | ?已实现 | 低 | `SendMessageBack()` 消费失败时将消息回退给 Broker 的 RETRY Topic |
| **顺序消费锁定** | ?已实现 | 中 | `LockBatchMQAsync()` / `UnlockBatchMQAsync()` / `OrderConsume` 属性 |
| **按时间戳搜索偏移** | ?已实现 | 低 | `SearchOffset()` / `SEARCH_OFFSET_BY_TIMESTAMP` |
| **消息查询（按ID）** | ?已实现 | 低 | `ViewMessage()` / `VIEW_MESSAGE_BY_ID` |
| **消息查询（按Key）** | ?已实现 | 低 | `QueryMessageByKey()` / `QUERY_MESSAGE` |
| **IPv6 支持** | ?已实现 | 中 | SysFlag 第2位判断IPv4/IPv6，自动适配地址长度和MsgId格式 |

### 8.2 中优先级缺失功能

| 功能 | 重要性 | 实现难度 | 说明 |
|------|:------:|:------:|------|
| SQL92 过滤 | ?已实现 | 低 | `ExpressionType="SQL92"` + SQL表达式 |
| 发送端消息压缩 | ?已实现 | 低 | `CompressOverBytes` 超过阈值自动ZLIB压缩 |
| 多Topic订阅 | ?已实现 | 低 | `Topics` 属性支持多主题订阅，Rebalance 按 Topic 分别获取 Broker 构建队列 |
| 广播模式本地偏移 | ?已实现 | 中 | `OffsetStorePath` 本地JSON文件持久化 |
| 死信队列 | ?已实现 | 低 | 超过重试次数后自动进入 `%DLQ%` Topic |
| 消费限流 | ?已实现 | 中 | `MaxConcurrentConsume` 信号量控制所有队列总并发 |
| Pop 消费模式 | ?已实现 | 中 | `PopMessageAsync()` / `AckMessageAsync()` / `ChangeInvisibleTimeAsync()` |
| 消费组管理 | ?已实现 | 低 | `CreateSubscriptionGroup()` / `DeleteSubscriptionGroup()` |

### 8.3 低优先级功能

| 功能 | 重要性 | 说明 |
|------|:------:|------|
| VIP 通道 | ★ | 使用 `BrokerPort - 2` 的VIP端口 |
| 消息过滤服务器 | ★ | `REGISTER_FILTER_SERVER` |
| 集群信息查询 | ★ | 管理功能 |
| Compaction Topic | ★ | 5.x 新特性 |
| gRPC 协议支持 | ?已实现 | — | 内置 Protobuf 编解码 + HTTP/2 gRPC，通过 `GrpcProxyAddress` 属性启用 |

---

## 九、协议层技术细节分析

### 9.1 协议帧格式（已正确实现）

```
+--------+----------------+--------+---------+
| Length  | HeaderLength   | Header | Body    |
| 4 bytes | 4 bytes        | N bytes| M bytes |
+--------+----------------+--------+---------+

HeaderLength 高 8 位: SerializeType (0=JSON, 1=ROCKETMQ)
HeaderLength 低 24 位: 实际 Header 长度
Length = 4 + N + M
```

### 9.2 已知协议兼容性风险

1. **版本声明**：默认 `V4_9_7`，`MQVersion` 枚举已扩展到 `V5_9_9` 和 `HIGHER_VERSION`。可通过 `Version` 属性自定义。
2. **Language 标识为 CPP**：`Header.Language` 默认 `"CPP"`，在 `OnBuild` 中改为 `"DOTNET"`。DOTNET 不在官方 Java 枚举中，部分 Broker 可能不识别。
3. **消息属性分隔符**：使用 `\x01` 和 `\x02` 分隔键值对，与 Java 官方一致。
4. **IPv6 消息格式**：RocketMQ 4.5+ 支持 IPv6，消息体中 IP 字段从 4 字节扩展到 16 字节（由 SysFlag 第2位标识），已支持自动识别和解析。

---

## 十、总结与建议

### 10.1 当前状态评估

NewLife.RocketMQ 作为纯托管客户端，已实现了 RocketMQ 4.x 的**绝大部分核心功能**，能够满足生产环境的消息生产和消费场景。在阿里云 4.x 实例和 Apache RocketMQ 4.x/5.x（Remoting模式）上有良好的适配。

**优势**：
- 纯 .NET 实现，无 Java/gRPC 外部依赖
- 同时支持 Remoting 协议（4.x）和 gRPC Proxy 协议（5.x）
- 支持多目标框架（net45+），gRPC 功能在 netstandard2.1+ 可用
- 代码简洁，核心路径清晰
- 已有阿里云 + Apache ACL + 华为云 + 腾讯云四种认证方式
- 支持 SSL/TLS
- 内置消息轨迹
- 内置 Request-Reply 模式
- 性能追踪（Tracer）集成
- **消费重试 + 死信队列**完整机制
- **事务回查回调**支持
- **批量消息发送**
- **消息压缩**（发送端）
- **SQL92 过滤**
- **多 Topic 订阅**
- **完整管理功能**（Topic/消费组 CRUD、消息查询、集群信息）
- **gRPC Proxy 协议支持**（发送/接收/确认/路由/心跳/事务/任意延迟）

**不足**：
- 批量消息解码（BatchMessage Body 内嵌多条消息）仅支持基本场景

### 10.2 建议的功能优先级路线图

**Phase 1 — 生产可靠性增强**（? 已完成）：
1. ? 消费重试机制（RETRY Topic + `CONSUMER_SEND_MSG_BACK`）
2. ? 事务回查回调（`CHECK_TRANSACTION_STATE` 响应）
3. ? 批量消息发送（`SEND_BATCH_MESSAGE`）
4. ? 顺序消费锁定（`LOCK_BATCH_MQ` / `UNLOCK_BATCH_MQ` / `OrderConsume`）

**Phase 2 — 功能完善**（? 已完成）：
5. ? 按时间戳搜索偏移
6. ? 消息查询（按ID / 按Key）
7. ? SQL92 过滤
8. ? IPv6 支持（SysFlag第2位自动识别）
9. ? 发送端消息压缩
10. ? 广播模式本地偏移持久化（`OffsetStorePath`）
11. ? 消费限流（`MaxConcurrentConsume` 信号量控制）
12. ? Pop 消费模式（`PopMessageAsync` / `AckMessageAsync` / `ChangeInvisibleTimeAsync`）
13. ? Broker 主从切换（`BrokerInfo.MasterAddress` / `SlaveAddresses`）

**Phase 3 — 专有版本支持**（? 已完成）：
14. ? 云厂商适配器统一接口（`ICloudProvider`）
15. ? 阿里云适配器（`AliyunProvider`）
16. ? 华为云适配器（`HuaweiProvider`）
17. ? 腾讯云适配器（`TencentProvider`）
18. ? Apache ACL 适配器（`AclProvider`）
19. ?? 阿里云 5.x 实例兼容性验证（待测试）
20. ?? MQVersion 已扩展到 V5_9_9，与 5.x Broker 通信兼容性待验证

**Phase 4 — 新架构支持**（? 已完成）：
21. ? 任意时间延迟消息（gRPC `PublishDelayViaGrpcAsync()`）
22. ? gRPC Proxy 协议支持（内置 Protobuf 编解码 + HTTP/2 gRPC 客户端，netstandard2.1+）
23. ? 多 Topic 订阅（`Topics` 属性 + 按 Topic 分别 Rebalance）
24. ? 批量消息解码（`MessageExt.ReadBatch()` 解码 BatchMessage Body）

---

## 附录 A：文件清单

| 文件 | 说明 |
|------|------|
| `MqBase.cs` | 业务基类，NameServer连接、Broker管理、配置 |
| `Producer.cs` | 生产者：普通/异步/单向/延迟/事务/Request-Reply |
| `Consumer.cs` | 消费者：Pull/调度/Rebalance/偏移管理 |
| `NameClient.cs` | NameServer客户端：路由发现 |
| `BrokerClient.cs` | Broker客户端：心跳、注销 |
| `ClusterClient.cs` | 集群通信：连接管理、签名、命令收发 |
| `AliyunOptions.cs` | 阿里云参数 |
| `AclOptions.cs` | Apache ACL 参数 |
| `ICloudProvider.cs` | 云厂商适配器统一接口 |
| `AliyunProvider.cs` | 阿里云适配器（`ICloudProvider` 实现） |
| `AclProvider.cs` | Apache ACL 适配器（`ICloudProvider` 实现） |
| `HuaweiProvider.cs` | 华为云适配器（`ICloudProvider` 实现） |
| `TencentProvider.cs` | 腾讯云适配器（`ICloudProvider` 实现） |
| `Grpc/IProtoMessage.cs` | Protobuf 消息接口 |
| `Grpc/ProtoWriter.cs` | 轻量级 Protobuf 二进制编码器 |
| `Grpc/ProtoReader.cs` | 轻量级 Protobuf 二进制解码器 |
| `Grpc/GrpcClient.cs` | gRPC 传输层（HTTP/2 + 帧编码，netstandard2.1+） |
| `Grpc/GrpcEnums.cs` | gRPC 枚举定义（GrpcCode/GrpcMessageType 等） |
| `Grpc/GrpcModels.cs` | gRPC 消息模型（Resource/Endpoints/Message/SystemProperties 等） |
| `Grpc/GrpcServiceMessages.cs` | gRPC 服务请求/响应消息类型 |
| `Grpc/GrpcMessagingService.cs` | RocketMQ 5.x gRPC 消息服务客户端 |
| `Protocol/Command.cs` | 协议帧编解码 |
| `Protocol/MqCodec.cs` | 网络层编解码器 |
| `Protocol/Header.cs` | 通信头 |
| `Protocol/Message.cs` | 消息模型 |
| `Protocol/MessageExt.cs` | 消息扩展（含二进制解码） |
| `Protocol/RequestCode.cs` | 请求码枚举（约60个） |
| `Protocol/ResponseCode.cs` | 响应码枚举 |
| `Protocol/SendMessageRequestHeader.cs` | 发送消息请求头 |
| `Protocol/PullMessageRequestHeader.cs` | 拉取消息请求头 |
| `Protocol/EndTransactionRequestHeader.cs` | 结束事务请求头 |
| `Protocol/TransactionState.cs` | 事务状态枚举 |
| `Protocol/MQVersion.cs` | 协议版本枚举（3.0 ~ 5.9，含 HIGHER_VERSION） |
| `MessageTrace/AsyncTraceDispatcher.cs` | 消息轨迹分发器 |
| `MessageTrace/MessageTraceHook.cs` | 消息轨迹钩子 |
| `Common/ILoadBalance.cs` | 负载均衡接口 |
| `Common/WeightRoundRobin.cs` | 加权轮询 |
| `Models/DelayTimeLevels.cs` | 延迟等级枚举 |
| `Models/MessageModels.cs` | 消息模型枚举 |
| `Models/ConsumeTypes.cs` | 消费类型枚举 |

## 附录 B：各厂商 RocketMQ 产品对比

| 厂商 | 产品 | 协议 | 认证方式 | NameServer发现 |
|------|------|------|---------|--------------|
| Apache | RocketMQ 4.x | Remoting (TCP) | ACL (AccessKey) | 直连/HTTP |
| Apache | RocketMQ 5.x | Remoting + gRPC | ACL | 直连/HTTP/Proxy |
| 阿里云 | 消息队列 RocketMQ 4.x | Remoting | 阿里云 AK/SK + HMAC-SHA1 | HTTP 接口 |
| 阿里云 | 消息队列 RocketMQ 5.x | gRPC 为主 | 阿里云 AK/SK | SDK/HTTP |
| 华为云 | DMS for RocketMQ | Remoting (4.x兼容) | SASL / AK/SK | 实例内网/公网 |
| 腾讯云 | TDMQ RocketMQ | Remoting (4.x兼容) | HMAC-SHA1 | VPC内网地址 |
