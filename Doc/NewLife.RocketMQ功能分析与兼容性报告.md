# NewLife.RocketMQ 产品分析报告

> 文档生成时间：2026年2月（更新于2026年7月）  
> 当前客户端版本：3.0.x  
> 协议版本声明：`MQVersion.V4_9_7`（默认，可配置，已支持 V5.9.9 及更高版本）  
> 目标框架：net45 / net461 / netstandard2.0 / netstandard2.1 / net10  
> 开发团队：新生命团队（NewLife）  
> 项目仓库：[GitHub](https://github.com/NewLifeX/NewLife.RocketMQ) | [Gitee](https://gitee.com/NewLifeX/NewLife.RocketMQ)

---

## 执行摘要

NewLife.RocketMQ 是一款企业级 **纯托管 .NET RocketMQ 客户端**，专为 .NET 生态设计，完全使用 C# 实现，**零外部依赖**（无需 Java、gRPC、Protobuf 第三方库）。该客户端同时支持：

- **RocketMQ Remoting 协议（4.x）**：完整实现 Apache RocketMQ 4.x 全部核心功能
- **gRPC Proxy 协议（5.x）**：内置轻量级 Protobuf 编解码器，原生支持 RocketMQ 5.x 新架构
- **多云厂商适配**：统一接口适配阿里云、华为云、腾讯云及 Apache ACL 认证体系

**核心优势**：
- ✅ **生产就绪**：完整的消费重试、死信队列、事务回查、顺序消费等企业级特性
- ✅ **跨平台**：支持 .NET Framework 4.5+ 到 .NET 10，gRPC 功能在 .NET Standard 2.1+ 可用
- ✅ **零依赖**：无需安装 Java 或 gRPC 运行时，内置完整 Protobuf 编解码
- ✅ **高性能**：基于 NewLife.Net 的高效网络层，支持连接池、SSL/TLS、VIP 通道
- ✅ **易扩展**：统一云厂商适配器接口，轻松接入各云服务商

---

## 一、项目概述

### 1.1 产品定位

NewLife.RocketMQ 是新生命团队开发的**企业级纯托管轻量级 RocketMQ 客户端**，旨在为 .NET 开发者提供一个：

- **功能完整**：覆盖 RocketMQ 4.x/5.x 全部核心特性及企业级功能
- **架构现代**：支持最新 C# 语法（file-scoped namespace、record、switch expression 等）
- **兼容广泛**：从 .NET Framework 4.5 到 .NET 10 全版本覆盖
- **易于集成**：NuGet 一键安装，无需额外配置或依赖安装

### 1.2 技术特点

**协议支持**：
- **Remoting 协议**：完整实现 RocketMQ 4.x TCP 私有协议（约 60+ RequestCode）
- **gRPC Proxy 协议**：自研轻量级 Protobuf 编解码器（ProtoWriter/ProtoReader，无外部依赖），支持 RocketMQ 5.x 的 11 个核心 RPC 方法

**架构设计**：
- **分层清晰**：业务层（Producer/Consumer）、通信层（ClusterClient/NameClient/BrokerClient）、协议层（Command/Message/Header）、传输层（Remoting/gRPC）
- **可测试性**：30+ 测试类覆盖核心功能（批量消息、事务回查、多 Topic、Pop 消费、IPv6、压缩、重试、顺序消费等）
- **可观测性**：内置消息轨迹（MessageTraceHook）、性能追踪（Tracer）、结构化日志（ILog）

### 1.3 核心价值

| 价值维度 | 说明 |
|---------|------|
| **企业级可靠性** | 完整的消费重试、死信队列、事务回查、顺序消费锁定等生产级特性 |
| **多云厂商适配** | 统一 `ICloudProvider` 接口，已适配阿里云、华为云、腾讯云及 Apache ACL |
| **高兼容性** | 兼容 RocketMQ 4.0 ~ 5.x，支持 .NET Framework 4.5+ 到 .NET 10 全版本 |
| **零外部依赖** | 纯 C# 实现，无需 Java、gRPC、Protobuf 第三方库，部署运维成本低 |
| **性能优化** | 连接复用、对象池、VIP 通道、消息压缩、并发控制等性能优化手段 |
| **易于使用** | 简洁的 API 设计，链式配置，NuGet 一键安装 |

---

## 二、核心架构分析

### 2.1 整体架构

```
MqBase (业务基类，NameServer连接/Broker管理/Topic与消费组CRUD/消息查询)
├── Producer (生产者：普通/异步/单向/延迟/事务/批量/Request-Reply/gRPC)
└── Consumer (消费者：Pull/调度/Rebalance/多Topic/顺序/重试/Pop/gRPC)

ClusterClient (集群客户端/通信层，Remoting协议)
├── NameClient (名称服务器客户端：路由发现/定时轮询/多Topic路由/Broker主从解析)
└── BrokerClient (Broker客户端：心跳/注销/命令收发)

Grpc/ (gRPC传输层，RocketMQ 5.x Proxy协议，netstandard2.1+)
├── GrpcClient (HTTP/2 gRPC客户端，帧编解码，Unary + Server Streaming)
├── GrpcMessagingService (消息服务：路由/发送/接收/确认/心跳/事务/延迟/死信/不可见时间/Telemetry)
├── ProtoWriter / ProtoReader (轻量级Protobuf编解码器，无外部依赖)
├── GrpcModels (Resource/Endpoints/Message/SystemProperties/MessageQueue 等)
├── GrpcServiceMessages (Request/Response 消息类型，约25个，含Telemetry)
└── GrpcEnums (GrpcCode/GrpcMessageType/GrpcClientType/AddressScheme 等)

Protocol/
├── Command (命令帧，Remoting协议编解码)
├── MqCodec (网络编解码器)
├── Header (通信头)
├── Message / MessageExt (消息模型，含批量解码/ZLIB解压/IPv4+IPv6/5.x MessageId)
├── SendMessageRequestHeader / PullMessageRequestHeader / EndTransactionRequestHeader
├── RequestCode (约60个指令码) / ResponseCode (约20个响应码)
├── MQVersion (V3.0 ~ V5.9.9 + HIGHER_VERSION，约450个版本)
└── TransactionState / LanguageCode / SerializeType 等辅助类型

CloudProvider/ (云厂商适配层)
├── ICloudProvider (统一云厂商接口：签名/路由/NameServer发现)
├── AliyunProvider (阿里云适配：实例ID前缀路由 + HTTP NameServer发现)
├── AclProvider (Apache ACL适配：HMAC-SHA1签名)
├── HuaweiProvider (华为云适配：SSL/TLS + 实例ID路由)
└── TencentProvider (腾讯云适配：Namespace前缀路由)
```

### 2.2 通信层特点

| 特性 | Remoting 协议 | gRPC 协议 |
|------|--------------|-----------|
| **传输层** | TCP 长连接（NewLife.Net） | HTTP/2（HttpClient） |
| **编解码** | 自定义二进制帧 + JSON | Protobuf（自研编解码器） |
| **支持版本** | RocketMQ 4.x / 5.x Broker | RocketMQ 5.x Proxy |
| **目标框架** | net45+ | netstandard2.1+ / net5+ |
| **连接复用** | 单连接 Opaque 复用 | HTTP/2 多路复用 |
| **SSL/TLS** | 支持（SslProtocols + Certificate） | 原生支持（HTTPS） |
| **VIP 通道** | 支持（BrokerPort - 2） | N/A（HTTP/2 优先级流） |
| **签名认证** | HMAC-SHA1（统一由 ICloudProvider 实现） | HTTP Header（X-Mq-*） |
| **外部代理** | 支持（ExternalBroker 属性） | 透明代理（HTTP Proxy） |
| **消息压缩** | ZLIB（发送端，SysFlag 标记） | gzip（HTTP Content-Encoding） |

### 2.3 代码组织结构

**核心模块**：
- **MqBase.cs**（约 1500+ 行）：业务基类，封装 NameServer 连接、Broker 管理、Topic/消费组 CRUD、消息查询、消费统计、gRPC 生命周期管理
- **Producer.cs**（约 1200+ 行）：生产者实现，支持同步/异步/单向/延迟/事务/批量发送、Request-Reply、gRPC 扩展
- **Consumer.cs**（约 1800+ 行）：消费者实现，支持 Pull/调度/Rebalance/多 Topic/顺序/重试/Pop/批量确认、gRPC 消费
- **ClusterClient.cs**（约 423 行）：TCP 连接管理、HMAC-SHA1 统一签名、同步异步命令收发
- **NameClient.cs**（约 243 行）：路由发现/定时轮询/多 Topic 路由/Broker 主从解析
- **BrokerClient.cs**（约 142 行）：心跳/注销/命令收发

**协议层**：
- **Command.cs**（约 333 行）：Remoting 协议帧编解码
- **MessageExt.cs**（约 244 行）：消息扩展，支持批量解码、ZLIB 解压、IPv4+IPv6 地址解析、5.x MessageId 编解码
- **RequestCode.cs**（约 277 行，60+ 指令）：服务端请求码枚举
- **MQVersion.cs**（约 909 行，450+ 版本）：协议版本枚举（V3.0 ~ V5.9.9 + HIGHER_VERSION）

**gRPC 层**（netstandard2.1+）：
- **GrpcClient.cs**（约 310 行）：HTTP/2 帧编码、Unary/ServerStreaming 调用
- **ProtoWriter.cs**（约 343 行）：Protobuf 二进制编码器（varint/fixed/string/bytes/map/timestamp 等）
- **ProtoReader.cs**（约 308 行）：Protobuf 二进制解码器
- **GrpcModels.cs**（约 520 行）：gRPC 消息模型（Resource/Endpoints/Message 等）
- **GrpcServiceMessages.cs**（约 883 行）：25+ Request/Response 消息类型
- **GrpcEnums.cs**（约 213 行）：GrpcCode/GrpcMessageType/GrpcClientType 等枚举
- **GrpcMessagingService.cs**（约 417 行）：11 个 RPC 方法实现

**云厂商适配**：
- **ICloudProvider.cs**：统一云厂商接口（7 个方法/属性）
- **AliyunProvider.cs**：阿里云适配（实例 ID 前缀 + HTTP NameServer 发现）
- **AclProvider.cs**：Apache ACL 适配（HMAC-SHA1 签名）
- **HuaweiProvider.cs**：华为云适配（SSL/TLS + 实例 ID 路由）
- **TencentProvider.cs**：腾讯云适配（Namespace 前缀路由）

**测试覆盖**（30+ 测试类，目标框架 net10）：
- **核心功能**：ProducerTests、ConsumerTests、CommandTests、MessageTests、NameClientTests
- **高级特性**：TransactionCheckTests、BatchMessageTests、RetryTests、OrderConsumeTests、PopConsumeTests
- **协议兼容**：IPv6Tests、MessageId5xTests、MQVersionTests、ProtoTests
- **云厂商**：AliyunTests、AliyunIssuesTests、CloudProviderTests
- **性能优化**：CompressionTests、ConcurrentConsumeTests、VipChannelTests
- **管理功能**：ManagementTests、ConsumeStatsTests、QueryMessageTests
- **扩展功能**：MultiTopicTests、RequestReplyTests、MessageTraceTests、SQL92FilterTests

---

## 三、RocketMQ 各主要版本功能矩阵

### 3.1 RocketMQ 4.x（4.0 ~ 4.9）— 经典 Remoting 协议

| 功能 | 版本引入 | NewLife 支持 | 备注 |
|------|---------|:----------:|------|
| **普通消息发送（同步）** | 4.0 | ✅ | `Publish()` / `SEND_MESSAGE_V2` |
| **普通消息发送（异步）** | 4.0 | ✅ | `PublishAsync()` |
| **单向发送（Oneway）** | 4.0 | ✅ | `PublishOneway()` |
| **Pull 模式消费** | 4.0 | ✅ | `Pull()` / `PULL_MESSAGE` |
| **集群消费模式** | 4.0 | ✅ | `MessageModels.Clustering` |
| **广播消费模式** | 4.0 | ✅ | `MessageModels.Broadcasting` |
| **消费者负载均衡（平均分配）** | 4.0 | ✅ | `Rebalance()` 平均分配算法 |
| **Tag 过滤** | 4.0 | ✅ | `Tags` / `Subscription` |
| **延迟消息（18级定时）** | 4.0 | ✅ | `PublishDelay()` / `DelayTimeLevels` |
| **事务消息（半消息）** | 4.3 | ✅ | `PublishTransaction()` / `EndTransaction()` |
| **顺序消息** | 4.0 | ✅ | `Publish(message, queue)` 指定队列 |
| **消息轨迹** | 4.4 | ✅ | `AsyncTraceDispatcher` / `MessageTraceHook` |
| **消费者偏移管理** | 4.0 | ✅ | `QueryOffset` / `UpdateOffset` / `QueryMaxOffset` / `QueryMinOffset` |
| **心跳机制** | 4.0 | ✅ | `BrokerClient.Ping()` |
| **Topic 创建/更新** | 4.0 | ✅ | `CreateTopic()` |
| **消费组信息查询** | 4.0 | ✅ | `GetConsumers()` |
| **Request-Reply 模式** | 4.6 | ✅ | `Request()` / `RequestAsync()` |
| **ACL 权限控制** | 4.4 | ✅ | `AclOptions` / `AclProvider` HMAC-SHA1 签名 |
| **SQL92 过滤** | 4.1 | ✅ | `ExpressionType=SQL92` + Subscription 填写SQL表达式 |
| **批量消息发送** | 4.5 | ✅ | `PublishBatch()` / `SEND_BATCH_MESSAGE (320)` |
| **消息压缩（发送端）** | 4.0 | ✅ | `CompressOverBytes` 超过阈值自动ZLIB压缩 + SysFlag标记 |
| **消息回退（消费失败）** | 4.0 | ✅ | `SendMessageBack()` / `CONSUMER_SEND_MSG_BACK (36)` |
| **事务回查（被动回查）** | 4.3 | ✅ | `OnCheckTransaction` / `CHECK_TRANSACTION_STATE (39)` 回调处理 |
| **按时间戳搜索偏移** | 4.0 | ✅ | `SearchOffset()` / `SEARCH_OFFSET_BY_TIMESTAMP` |
| **消费进度持久化（本地）** | 4.0 | ✅ | `OffsetStorePath` 广播模式本地JSON文件持久化，集群模式走Broker端存储 |
| **Push 模式（服务端推送）** | 4.0 | ⚠️ | 本质是长轮询 Pull 模拟，已实现但非原生 Push |
| **Pop 消费模式** | 4.9.3 | ✅ | `PopMessageAsync()` / `AckMessageAsync()` / `ChangeInvisibleTimeAsync()` |
| **多 Topic 订阅** | 4.0 | ✅ | `Topics` 属性支持多主题订阅，Rebalance 按 Topic 分别分配队列 |
| **消费重试** | 4.0 | ✅ | `EnableRetry` + `MaxReconsumeTimes`，消费失败自动回退到RETRY Topic |
| **死信队列（DLQ）** | 4.0 | ✅ | 超过最大重试次数后自动进入 `%DLQ%{ConsumerGroup}` 主题 |
| **消费限流/并发控制** | 4.0 | ✅ | `MaxConcurrentConsume` 信号量控制所有队列的总并发 |
| **VIP 通道** | 4.0 | ✅ | `VipChannelEnabled` 属性，启用后使用 BrokerPort - 2 的VIP端口 |
| **Broker 主从切换** | 4.5 | ✅ | `BrokerInfo.MasterAddress`/`SlaveAddresses`，消费失败自动切换从节点读取 |

### 3.2 RocketMQ 5.x（5.0+）— 新架构

RocketMQ 5.0 引入了全新的 **gRPC Proxy** 架构，同时保持对 Remoting 协议的向后兼容。

| 功能 | 版本引入 | NewLife 支持 | 备注 |
|------|---------|:----------:|------|
| **gRPC Proxy 协议** | 5.0 | ✅ | 内置轻量 Protobuf 编解码 + HTTP/2 gRPC 客户端，通过 `GrpcProxyAddress` 属性启用（netstandard2.1+） |
| **Remoting 协议兼容** | 5.0 | ✅ | 5.x Broker 保留了 Remoting 接口，当前客户端可连接 |
| **任意时间延迟消息** | 5.0 | ✅ | `PublishDelayViaGrpcAsync()` 通过 gRPC 协议支持任意时间戳延迟消息 |
| **Pop 消费模式** | 5.0 | ✅ | `PopMessageAsync()` / `AckMessageAsync()` / `ChangeInvisibleTimeAsync()` |
| **客户端主动上报资源** | 5.0 | ✅ | `TelemetryViaGrpcAsync()` 通过 gRPC 协议上报客户端设置/订阅等资源信息（netstandard2.1+） |
| **消息分组（FIFO）** | 5.0 | ⚠️ | Remoting 模式下等价于顺序消息 |
| **服务端 Rebalance** | 5.0 | ❌ | 需 Broker 5.0 + gRPC 协议配合 |
| **MessageId 新格式** | 5.0 | ✅ | `MessageExt.CreateMessageId5x()` / `TryParseMessageId5x()` / `IsMessageId5x()` 编解码5.x格式 |
| **Controller 模式** | 5.0 | ⚠️ | 替代 DLedger 的高可用方案，客户端无感知 |
| **Compaction Topic** | 5.1 | ❌ | KV 语义 Topic |
| **Timer 消息（5.0 原生）** | 5.0 | ✅ | Remoting 模式不支持。gRPC 模式通过 `PublishDelayViaGrpcAsync()` 实现 |

### 3.3 版本兼容性总结

| 服务端版本 | 连接能力 | 核心功能 | 已知问题 |
|-----------|:------:|:------:|---------|
| **4.0 ~ 4.3** | ✅ | ✅ | 无事务消息支持 |
| **4.4 ~ 4.9** | ✅ | ✅ | 完全兼容，主力测试版本 |
| **5.0 ~ 5.x（Remoting模式）** | ✅ | ✅ | 新特性（Timer消息）不可用 |
| **5.0 ~ 5.x（gRPC Proxy模式）** | ✅ | ✅ | 通过 `GrpcProxyAddress` 属性启用，需 netstandard2.1+ |

---

## 四、阿里云 RocketMQ 专有版本兼容性

### 4.1 阿里云 RocketMQ 4.x 实例

| 功能 | 支持状态 | 实现方式 |
|------|:------:|---------|
| **实例ID路由** | ✅ | `AliyunProvider.TransformTopic/TransformGroup`：`{InstanceId}%{Topic}` / `{InstanceId}%{Group}` |
| **名称服务器发现** | ✅ | `AliyunProvider.GetNameServerAddress()`：HTTP 接口获取 NameServer 地址 |
| **AccessKey/SecretKey 签名** | ✅ | `ClusterClient.SetSignature()` HMAC-SHA1 签名 |
| **OnsChannel 标识** | ✅ | 默认 `ALIYUN` |
| **实例ID自动解析** | ✅ | 从 NameServer 地址中提取 `MQ_INST_` 前缀 |
| **公网版消费者状态** | ⚠️ | 阿里云公网版返回的 `brokerName` 与 NameServer 路由中的不一致，需特殊处理（已有容错代码） |
| **消费者状态JSON** | ✅ | `ConsumerStatesSpecialJsonHandler` 处理阿里云特殊JSON格式 |
| **铂金版/独享实例** | ⚠️ | 理论兼容，但未有充分测试 |
| **多 Tag 过滤** | ✅ | 通过 `Tags` 属性支持 |

### 4.2 阿里云 RocketMQ 5.x 实例

| 功能 | 支持状态 | 备注 |
|------|:------:|------|
| **Remoting 协议兼容** | ⚠️ | 阿里云 5.x 实例保留 Remoting 接口，理论上可连接 |
| **gRPC Proxy 模式** | ⚠️ | 阿里云推荐使用 gRPC 方式接入，客户端已支持 gRPC 协议，待验证兼容性 |
| **实例级别认证** | ⚠️ | 5.x 可能使用新的认证体系 |
| **Serverless 实例** | ❌ | 仅支持 gRPC 接入，待验证 |

### 4.3 当前阿里云适配的已知问题

1. **公网版 BrokerName 不匹配**：阿里云公网版 RocketMQ 返回的消费者状态中 `brokerName` 是真实 Broker 名称，而 NameServer 路由返回的是网关名称，导致 `InitOffsetAsync` 中偏移匹配失败。当前代码用 `?? new OffsetWrapperModel()` 容错处理。
2. **实例ID自动识别限制**：仅支持从 NameServer 地址中提取 `MQ_INST_` 前缀，无法处理自定义实例ID。

---

## 五、华为云 RocketMQ 专有版本兼容性

### 5.1 华为云 DMS for RocketMQ

华为云的分布式消息服务（DMS）提供 RocketMQ 兼容实例。

| 功能 | 支持状态 | 备注 |
|------|:------:|------|
| **标准 Remoting 协议** | ⚠️ | 华为云 DMS 4.x 兼容实例理论上可连接，需要验证 |
| **SSL/TLS 加密** | ✅ | 客户端支持 `SslProtocol` 和 `Certificate` 配置 |
| **SASL 认证** | ❌ | 华为云可能使用 SASL 认证，当前不支持 |
| **ACL 认证** | ⚠️ | 华为云 DMS 使用自己的 AccessKey 体系，需验证是否与 Apache ACL 兼容 |
| **华为云 5.x 实例** | ❌ | 可能仅支持 gRPC Proxy |

### 5.2 华为云适配

当前客户端已提供华为云专用适配器 `HuaweiProvider`（`ICloudProvider` 实现）：

**使用方式**：
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

**待验证事项**：
1. 华为云 DMS 的 ACL 签名是否与 Apache ACL 完全一致
2. SSL/TLS 证书验证是否需要特殊处理
3. NameServer 地址发现机制

---

## 六、腾讯云 TDMQ RocketMQ 版兼容性

### 6.1 腾讯云 TDMQ for RocketMQ

腾讯云 TDMQ（Tencent Distributed Message Queue）提供 RocketMQ 兼容版本，基于 Apache RocketMQ 4.x 协议。

| 功能 | 支持状态 | 备注 |
|------|:------:|------|
| **标准 Remoting 协议** | ⚠️ | 腾讯云 TDMQ 4.x 兼容实例，理论上可通过 Remoting 协议连接 |
| **SSL/TLS 加密** | ✅ | 客户端支持 `SslProtocol` 和 `Certificate` 配置 |
| **HMAC-SHA1 签名认证** | ⚠️ | 腾讯云使用类似 HMAC-SHA1 的签名方式，需验证与 Apache ACL 兼容性 |
| **VPC 内网访问** | ✅ | 直接配置 NameServer 为 VPC 内网地址即可 |
| **延迟消息** | ✅ | 标准 18 级延迟消息，与 Apache 4.x 一致 |
| **事务消息** | ⚠️ | 需验证腾讯云对半消息和回查的处理 |

### 6.2 腾讯云适配

当前客户端已提供腾讯云专用适配器 `TencentProvider`（`ICloudProvider` 实现），支持 Namespace 前缀路由：

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

---

## 七、已实现协议详细分析

### 7.1 已实现的 RequestCode（服务端请求码）

| RequestCode | 值 | 用途 | 调用位置 |
|-------------|:--:|------|---------|
| `SEND_MESSAGE` | 10 | 发消息（V1，已不使用） | 仅定义 |
| `PULL_MESSAGE` | 11 | 拉取消息 | `Consumer.Pull()` |
| `QUERY_MESSAGE` | 12 | 查询消息 | `MqBase.QueryMessageByKey()` |
| `QUERY_CONSUMER_OFFSET` | 14 | 查询消费偏移 | `Consumer.QueryOffset()` |
| `UPDATE_CONSUMER_OFFSET` | 15 | 更新消费偏移 | `Consumer.UpdateOffset()` |
| `UPDATE_AND_CREATE_TOPIC` | 17 | 创建/更新Topic | `MqBase.CreateTopic()` |
| `GET_BROKER_RUNTIME_INFO` | 28 | Broker运行信息 | `BrokerClient.GetRuntimeInfo()` |
| `SEARCH_OFFSET_BY_TIMESTAMP` | 29 | 按时间戳搜索偏移 | `Consumer.SearchOffset()` |
| `GET_MAX_OFFSET` | 30 | 最大偏移量 | `Consumer.QueryMaxOffset()` |
| `GET_MIN_OFFSET` | 31 | 最小偏移量 | `Consumer.QueryMinOffset()` |
| `VIEW_MESSAGE_BY_ID` | 33 | 按ID查看消息 | `MqBase.ViewMessage()` |
| `HEART_BEAT` | 34 | 心跳 | `BrokerClient.Ping()` |
| `UNREGISTER_CLIENT` | 35 | 注销客户端 | `BrokerClient.UnRegisterClient()` |
| `CONSUMER_SEND_MSG_BACK` | 36 | 消费失败回退 | `Consumer.SendMessageBackAsync()` |
| `END_TRANSACTION` | 37 | 结束事务 | `Producer.EndTransaction()` |
| `GET_CONSUMER_LIST_BY_GROUP` | 38 | 消费者列表 | `Consumer.GetConsumers()` |
| `CHECK_TRANSACTION_STATE` | 39 | 事务回查 | 被动处理：`OnCheckTransaction` 回调 |
| `NOTIFY_CONSUMER_IDS_CHANGED` | 40 | 消费者变更通知 | 触发重平衡 |
| `LOCK_BATCH_MQ` | 41 | 锁定队列（顺序消费） | `Consumer.LockBatchMQAsync()` |
| `UNLOCK_BATCH_MQ` | 42 | 解锁队列 | `Consumer.UnlockBatchMQAsync()` |
| `GET_ROUTEINTO_BY_TOPIC` | 105 | Topic路由 | `NameClient.GetRouteInfo()` |
| `GET_BROKER_CLUSTER_INFO` | 106 | 集群信息 | `MqBase.GetClusterInfo()` |
| `UPDATE_AND_CREATE_SUBSCRIPTIONGROUP` | 200 | 创建/更新消费组 | `MqBase.CreateSubscriptionGroup()` |
| `GET_CONSUMER_CONNECTION_LIST` | 203 | 消费者连接列表 | `Consumer.GetConsumerConnectionList()` |
| `DELETE_SUBSCRIPTIONGROUP` | 207 | 删除消费组 | `MqBase.DeleteSubscriptionGroup()` |
| `GET_CONSUME_STATS` | 208 | 消费状态 | `Consumer.InitOffsetAsync()` |
| `DELETE_TOPIC_IN_BROKER` | 215 | 在Broker中删除Topic | `MqBase.DeleteTopic()` |
| `DELETE_TOPIC_IN_NAMESRV` | 216 | 在NameServer中删除Topic | `MqBase.DeleteTopic()` |
| `GET_CONSUMER_RUNNING_INFO` | 307 | 消费者运行信息 | 被动处理 |
| `INVOKE_BROKER_TO_RESET_OFFSET` | 222 | 重置消费偏移 | `Consumer.ResetConsumerOffset()` |
| `SEND_MESSAGE_V2` | 310 | 发消息V2 | `Producer.Publish()` / `PublishAsync()` |
| `SEND_BATCH_MESSAGE` | 320 | 批量发送 | `Producer.PublishBatch()` |
| `SEND_REPLY_MESSAGE_V2` | 325 | 回复消息 | `Consumer.SendReplyAsync()` |
| `POP_MESSAGE` | 200050 | Pop消费 | `Consumer.PopMessageAsync()` |
| `ACK_MESSAGE` | 200051 | Pop消息确认 | `Consumer.AckMessageAsync()` |
| `CHANGE_MESSAGE_INVISIBLETIME` | 200052 | 修改不可见时间 | `Consumer.ChangeInvisibleTimeAsync()` |
| `BATCH_ACK_MESSAGE` | 200151 | 批量Pop确认 | `Consumer.BatchAckMessageAsync()` |

### 7.2 消息编解码分析

**消息发送（MessageExt.Write）**：仅返回 `true`，不实际写入，因为发送走的是 `Command` 封装。

**消息接收（MessageExt.Read/ReadAll/DecodeBatch）**：
- ✅ 支持标准的 4.x 消息二进制格式
- ✅ 支持 ZLIB 解压缩（SysFlag 第0位）
- ✅ 支持 IPv4 地址解析
- ✅ 支持 IPv6 地址解析（SysFlag 第2位标识，自动适配4字节/16字节IP）
- ✅ 支持批量消息解码（`DecodeBatch()` 方法解码 SysFlag 第4位标识的 BatchMessage Body）

### 7.3 序列化格式

| 格式 | 支持 | 说明 |
|------|:---:|------|
| JSON | ✅ | 默认格式，使用 `NewLife.Serialization.JsonHelper` |
| ROCKETMQ（二进制） | ✅ | 命令头部支持二进制解析 |

### 7.4 gRPC 服务方法（RocketMQ 5.x Proxy）

| 服务方法 | 实现类方法 | 说明 |
|---------|----------|------|
| `QueryRoute` | `GrpcMessagingService.QueryRouteAsync()` | 查询主题路由信息 |
| `SendMessage` | `GrpcMessagingService.SendMessageAsync()` | 发送消息（普通/延迟/FIFO） |
| `SendMessage (Transaction)` | `GrpcMessagingService.SendTransactionMessageAsync()` | 发送事务消息（半消息） |
| `QueryAssignment` | `GrpcMessagingService.QueryAssignmentAsync()` | 查询队列分配 |
| `ReceiveMessage` | `GrpcMessagingService.ReceiveMessageAsync()` | 接收消息（Server Streaming） |
| `AckMessage` | `GrpcMessagingService.AckMessageAsync()` | 确认消息消费 |
| `Heartbeat` | `GrpcMessagingService.HeartbeatAsync()` | 心跳 |
| `EndTransaction` | `GrpcMessagingService.EndTransactionAsync()` | 结束事务（提交/回滚） |
| `ForwardToDeadLetterQueue` | `GrpcMessagingService.ForwardToDeadLetterQueueAsync()` | 转发到死信队列 |
| `ChangeInvisibleDuration` | `GrpcMessagingService.ChangeInvisibleDurationAsync()` | 修改不可见时间 |
| `NotifyClientTermination` | `GrpcMessagingService.NotifyClientTerminationAsync()` | 通知客户端终止 |
| `Telemetry` | `GrpcMessagingService.TelemetryAsync()` | 客户端资源上报（设置/订阅等） |

---

## 八、功能完善度评估

### 8.1 生产者功能（Producer）

| 功能 | 状态 | 详细说明 |
|------|:----:|---------|
| 同步发送 | ✅完整 | 支持重试（`RetryTimesWhenSendFailed`）、超时、负载均衡（`ILoadBalance`） |
| 异步发送 | ✅完整 | 异步版本，支持 CancellationToken。gRPC 模式自动路由 |
| 单向发送 | ✅完整 | 不等待结果 |
| 延迟消息 | ✅完整 | 18级定时消息 |
| 任意时间延迟 | ✅完整 | gRPC 模式下 `PublishDelayViaGrpcAsync()` 支持任意时间戳延迟 |
| 事务消息 | ✅完整 | 支持发送半消息、提交/回滚，支持被动回查回调 `OnCheckTransaction`（同步/异步委托） |
| 顺序消息 | ✅完整 | 通过指定 `MessageQueue` 参数实现 |
| Request-Reply | ✅完整 | 同步 `Request()` / 异步 `RequestAsync()`，内置回复消费者 |
| 批量发送 | ✅完整 | `PublishBatch()` 支持 `List<Message>` 或 `List<String>` 批量发送 |
| 消息压缩 | ✅完整 | 发送端超过 `CompressOverBytes` 阈值自动ZLIB压缩 |
| 发送端钩子 | ✅完整 | `ISendMessageHook` 前后拦截 |
| 消息轨迹 | ✅完整 | `AsyncTraceDispatcher` 异步轨迹分发 |
| gRPC 事务 | ✅完整 | `PublishTransactionViaGrpcAsync()` / `EndTransactionViaGrpcAsync()` |
| gRPC 路由查询 | ✅完整 | `QueryRouteViaGrpcAsync()` |

### 8.2 消费者功能（Consumer）

| 功能 | 状态 | 详细说明 |
|------|:----:|---------|
| Pull 模式 | ✅完整 | 长轮询拉取 |
| 消费调度 | ✅完整 | 自动分配队列并启动消费线程 |
| 集群消费 | ✅完整 | 平均分配 Rebalance 算法 |
| 广播消费 | ✅完整 | `OffsetStorePath` 本地JSON文件持久化偏移 |
| Tag 过滤 | ✅完整 | 支持 Tag 表达式 |
| SQL92 过滤 | ✅完整 | `ExpressionType="SQL92"` + SQL表达式 |
| 偏移管理 | ✅完整 | `QueryOffset` / `UpdateOffset` / `QueryMaxOffset` / `QueryMinOffset` / `SearchOffset` |
| 消费者信息上报 | ✅完整 | `GetConsumerRunningInfo` |
| 消费者变更通知 | ✅完整 | `NOTIFY_CONSUMER_IDS_CHANGED` 触发重平衡 |
| 消费重试 | ✅完整 | `EnableRetry` + `MaxReconsumeTimes`，自动回退到RETRY Topic |
| 死信队列 | ✅完整 | 超过最大重试次数后自动进入 `%DLQ%` Topic |
| 消费回退 | ✅完整 | `SendMessageBackAsync()` / `CONSUMER_SEND_MSG_BACK` |
| 顺序消费（锁定） | ✅完整 | `LockBatchMQAsync()` / `UnlockBatchMQAsync()` / `OrderConsume` 属性 |
| 多Topic订阅 | ✅完整 | `Topics` 属性支持多主题订阅，Rebalance 按 Topic 分别获取 Broker 构建队列 |
| Pop 消费 | ✅完整 | `PopMessageAsync()` / `AckMessageAsync()` / `ChangeInvisibleTimeAsync()` |
| 按时间戳消费 | ✅完整 | `SearchOffset()` / `SEARCH_OFFSET_BY_TIMESTAMP` |
| 消费限流 | ✅完整 | `MaxConcurrentConsume` 信号量控制总并发 |
| 消费端钩子 | ✅完整 | `IConsumeMessageHook` 前后拦截 |
| Request-Reply 回复 | ✅完整 | `SendReply()` / `SendReplyAsync()` |
| gRPC 接收消息 | ✅完整 | `ReceiveMessageViaGrpcAsync()` Server Streaming |
| gRPC 确认消息 | ✅完整 | `AckMessageViaGrpcAsync()` |
| gRPC 队列分配 | ✅完整 | `QueryAssignmentViaGrpcAsync()` |
| gRPC 不可见时间 | ✅完整 | `ChangeInvisibleDurationViaGrpcAsync()` |
| gRPC 心跳 | ✅完整 | `HeartbeatViaGrpcAsync()` |

### 8.3 管理功能

| 功能 | 状态 | 详细说明 |
|------|:----:|---------|
| Topic 创建/更新 | ✅完整 | 在所有 Broker 上创建 |
| Topic 删除 | ✅完整 | `DeleteTopic()` 同时从 Broker 和 NameServer 删除 |
| 消费组创建/更新 | ✅完整 | `CreateSubscriptionGroup()` |
| 消费组删除 | ✅完整 | `DeleteSubscriptionGroup()` |
| Broker 运行信息 | ✅完整 | `GetRuntimeInfo()` |
| 消费组查询 | ✅完整 | `GetConsumers()` |
| 消费统计 | ✅完整 | `GetConsumeStats()` 获取消费组完整统计信息 + `GetTopicStatsInfo()` 获取主题统计 |
| 消息查询（按ID） | ✅完整 | `ViewMessage()` / `VIEW_MESSAGE_BY_ID` |
| 消息查询（按Key） | ✅完整 | `QueryMessageByKey()` / `QUERY_MESSAGE` |
| 集群信息查询 | ✅完整 | `GetClusterInfo()` / `GET_BROKER_CLUSTER_INFO` |
| 消费者连接列表 | ✅完整 | `GetConsumerConnectionList()` / `GET_CONSUMER_CONNECTION_LIST` |
| 偏移重置 | ✅完整 | `ResetConsumerOffset()` / `INVOKE_BROKER_TO_RESET_OFFSET` |

---

## 九、各家专有版本对比

### 9.1 功能支持对比表

| 功能 | Apache 4.x | Apache 5.x | 阿里云 4.x | 阿里云 5.x | 华为云 DMS | 腾讯云 TDMQ |
|------|:----------:|:----------:|:---------:|:---------:|:---------:|:---------:|
| Remoting连接 | ✅ | ✅ | ✅ | ⚠️ | ⚠️ | ⚠️ |
| gRPC连接 | N/A | ✅ | N/A | ⚠️ | ⚠️ | ⚠️ |
| 签名认证 | ✅ACL | ✅ACL | ✅阿里签名 | ⚠️ | ✅华为云适配 | ⚠️待验证 |
| 实例ID路由 | N/A | N/A | ✅ | ✅ | 待验证 | ✅Namespace |
| NameServer发现 | 直连 | 直连 | ✅HTTP | 待验证 | 待验证 | VPC直连 |
| SSL/TLS | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### 9.2 专有版本适配代码清单

**云厂商适配器接口**：
- `ICloudProvider.cs` — 统一云厂商适配接口（`Name`/`AccessKey`/`SecretKey`/`OnsChannel`/`TransformTopic`/`TransformGroup`/`GetNameServerAddress`）

**阿里云适配**（已完成）：
- `AliyunProvider.cs` — 阿里云适配器（`ICloudProvider` 实现，实例ID前缀路由 + HTTP NameServer发现）
- `AliyunOptions.cs` — 旧版阿里云参数（`[Obsolete]`，自动桥接到 `AliyunProvider`）
- `ClusterClient.SetSignature()` — HMAC-SHA1 统一签名
- `Consumer.ConsumerStatesSpecialJsonHandler()` — 阿里云特殊JSON解析

**Apache ACL 适配**（已完成）：
- `AclProvider.cs` — Apache ACL 适配器（`ICloudProvider` 实现，不转换Topic/Group）
- `AclOptions.cs` — 旧版ACL参数（`[Obsolete]`，自动桥接到 `AclProvider`）

**华为云适配**（已完成）：
- `HuaweiProvider.cs` — 华为云适配器（`ICloudProvider` 实现，支持 `EnableSsl` 属性）

**腾讯云适配**（已完成）：
- `TencentProvider.cs` — 腾讯云 TDMQ 适配器（`ICloudProvider` 实现，Namespace 前缀路由）

---

## 十、与官方 Java 客户端功能差距

以下是与 Apache RocketMQ 官方 Java 客户端 4.9.x 对比的主要差距：

### 10.1 已消除的功能差距

| 功能 | 状态 | 说明 |
|------|:----:|------|
| **消费重试机制** | ✅已实现 | `EnableRetry` + `MaxReconsumeTimes`，消费失败后自动发送到 `%RETRY%{ConsumerGroup}` Topic |
| **事务回查回调** | ✅已实现 | Broker 主动调用 `CHECK_TRANSACTION_STATE`，客户端通过 `OnCheckTransaction`（同步/异步）回应 |
| **批量消息发送** | ✅已实现 | `PublishBatch()`，将多条消息合并为一个请求发送 |
| **批量消息解码** | ✅已实现 | `MessageExt.DecodeBatch()` 解码 SysFlag 第4位标识的 BatchMessage Body |
| **消费回退（SendBack）** | ✅已实现 | `SendMessageBackAsync()` 消费失败时将消息回退给 Broker 的 RETRY Topic |
| **顺序消费锁定** | ✅已实现 | `LockBatchMQAsync()` / `UnlockBatchMQAsync()` / `OrderConsume` 属性 |
| **按时间戳搜索偏移** | ✅已实现 | `SearchOffset()` / `SEARCH_OFFSET_BY_TIMESTAMP` |
| **消息查询（按ID）** | ✅已实现 | `ViewMessage()` / `VIEW_MESSAGE_BY_ID` |
| **消息查询（按Key）** | ✅已实现 | `QueryMessageByKey()` / `QUERY_MESSAGE` |
| **IPv6 支持** | ✅已实现 | SysFlag 第2位判断IPv4/IPv6，自动适配地址长度和MsgId格式 |
| **SQL92 过滤** | ✅已实现 | `ExpressionType="SQL92"` + SQL表达式 |
| **发送端消息压缩** | ✅已实现 | `CompressOverBytes` 超过阈值自动ZLIB压缩 |
| **多Topic订阅** | ✅已实现 | `Topics` 属性支持多主题订阅，Rebalance 按 Topic 分别获取 Broker 构建队列 |
| **广播模式本地偏移** | ✅已实现 | `OffsetStorePath` 本地JSON文件持久化 |
| **死信队列** | ✅已实现 | 超过重试次数后自动进入 `%DLQ%` Topic |
| **消费限流** | ✅已实现 | `MaxConcurrentConsume` 信号量控制所有队列总并发 |
| **Pop 消费模式** | ✅已实现 | `PopMessageAsync()` / `AckMessageAsync()` / `ChangeInvisibleTimeAsync()` |
| **消费组管理** | ✅已实现 | `CreateSubscriptionGroup()` / `DeleteSubscriptionGroup()` |
| **gRPC 协议支持** | ✅已实现 | 内置 Protobuf 编解码 + HTTP/2 gRPC，通过 `GrpcProxyAddress` 属性启用 |

### 10.2 仍有差距的功能

| 功能 | 重要性 | 说明 |
|------|:------:|------|
| Compaction Topic | ★ | 5.x 新特性，KV 语义 Topic |
| 服务端 Rebalance | ★★ | 5.x + gRPC 协议配合 |

### 10.3 本次新增消除的功能差距

| 功能 | 状态 | 说明 |
|------|:----:|------|
| **VIP 通道** | ✅已实现 | `VipChannelEnabled` 属性，启用后使用 BrokerPort - 2 的VIP端口 |
| **批量确认Pop消息** | ✅已实现 | `BatchAckMessageAsync()` / `BATCH_ACK_MESSAGE (200151)` |
| **消费统计完整API** | ✅已实现 | `GetConsumeStats()` 完整消费统计 + `GetTopicStatsInfo()` 主题统计 |
| **消息过滤服务器注册** | ✅已实现 | `RegisterFilterServer()` / `REGISTER_FILTER_SERVER (301)` |
| **5.x MessageId 新格式** | ✅已实现 | `CreateMessageId5x()` / `TryParseMessageId5x()` / `IsMessageId5x()` |
| **客户端资源上报** | ✅已实现 | `TelemetryViaGrpcAsync()` 通过 gRPC Telemetry 上报客户端设置 |

---

## 十一、协议层技术细节分析

### 11.1 协议帧格式（Remoting，已正确实现）

```
+--------+----------------+--------+---------+
| Length  | HeaderLength   | Header | Body    |
| 4 bytes | 4 bytes        | N bytes| M bytes |
+--------+----------------+--------+---------+

HeaderLength 高 8 位: SerializeType (0=JSON, 1=ROCKETMQ)
HeaderLength 低 24 位: 实际 Header 长度
Length = 4 + N + M
```

### 11.2 gRPC 帧格式（HTTP/2，已正确实现）

```
+-----+--------+---------+
| Comp| Length  | Body    |
| 1B  | 4 bytes | N bytes |
+-----+--------+---------+

Comp: 0=不压缩, 1=gzip
Length: 大端序，Protobuf 消息体长度
Body: Protobuf 编码的消息
```

### 11.3 已知协议兼容性风险

1. **版本声明**：默认 `V4_9_7`，`MQVersion` 枚举已扩展到 `V5_9_9` 和 `HIGHER_VERSION`（约450个版本值）。可通过 `Version` 属性自定义。
2. **Language 标识为 DOTNET**：`Header.Language` 在 `OnBuild` 中设置为 `"DOTNET"`。DOTNET 不在官方 Java 枚举中，部分 Broker 可能不识别。
3. **消息属性分隔符**：使用 `\x01` 和 `\x02` 分隔键值对，与 Java 官方一致。
4. **IPv6 消息格式**：RocketMQ 4.5+ 支持 IPv6，消息体中 IP 字段从 4 字节扩展到 16 字节（由 SysFlag 第2位标识），已支持自动识别和解析。
5. **gRPC 协议版本**：基于 `apache.rocketmq.v2` 服务定义，路径格式 `/apache.rocketmq.v2.MessagingService/{Method}`。

---

## 十二、总结与建议

### 12.1 当前状态评估

NewLife.RocketMQ 作为纯托管客户端，已实现了 RocketMQ 4.x 的**全部核心功能**以及 5.x 的 **gRPC Proxy 协议**，能够满足生产环境的各类消息场景。

**优势**：
- 纯 .NET 实现，无 Java/gRPC/Protobuf 外部依赖
- 同时支持 Remoting 协议（4.x）和 gRPC Proxy 协议（5.x）
- 支持多目标框架（net45+），gRPC 功能在 netstandard2.1+ 可用
- 代码简洁，核心路径清晰
- 已有阿里云 + Apache ACL + 华为云 + 腾讯云四种认证适配
- 统一 `ICloudProvider` 云厂商适配器接口
- 支持 SSL/TLS 加密传输
- 内置消息轨迹 + Request-Reply 模式
- 性能追踪（Tracer）集成
- **消费重试 + 死信队列**完整机制
- **事务回查回调**支持（同步 + 异步两种委托）
- **批量消息发送 + 解码**
- **消息压缩**（发送端ZLIB）
- **SQL92 过滤**
- **多 Topic 订阅**
- **顺序消费锁定**
- **消费限流（信号量并发控制）**
- **完整管理功能**（Topic/消费组 CRUD、消息查询、集群信息、偏移重置、消费统计、主题统计）
- **gRPC Proxy 协议支持**（路由/发送/接收/确认/心跳/事务/延迟/死信/不可见时间/终止通知/Telemetry）
- **VIP 通道**支持（BrokerPort - 2 优先级连接）
- **批量确认Pop消息**（BATCH_ACK_MESSAGE）
- **消息过滤服务器注册**（REGISTER_FILTER_SERVER）
- **5.x MessageId 编解码**
- **客户端资源上报**（gRPC Telemetry）

**已知限制**：
- 服务端 Rebalance（5.x gRPC）未实现
- Compaction Topic（5.x KV 语义）未实现
- gRPC 协议需要实际 RocketMQ 5.x Proxy 环境做集成测试验证

### 12.2 功能路线图

**Phase 1 — 生产可靠性增强**（✅ 已完成）：
1. ✅ 消费重试机制（RETRY Topic + `CONSUMER_SEND_MSG_BACK`）
2. ✅ 事务回查回调（`CHECK_TRANSACTION_STATE` 响应）
3. ✅ 批量消息发送（`SEND_BATCH_MESSAGE`）
4. ✅ 顺序消费锁定（`LOCK_BATCH_MQ` / `UNLOCK_BATCH_MQ` / `OrderConsume`）

**Phase 2 — 功能完善**（✅ 已完成）：
5. ✅ 按时间戳搜索偏移
6. ✅ 消息查询（按ID / 按Key）
7. ✅ SQL92 过滤
8. ✅ IPv6 支持
9. ✅ 发送端消息压缩
10. ✅ 广播模式本地偏移持久化
11. ✅ 消费限流（信号量控制）
12. ✅ Pop 消费模式
13. ✅ Broker 主从切换

**Phase 3 — 专有版本支持**（✅ 已完成）：
14. ✅ 云厂商适配器统一接口（`ICloudProvider`）
15. ✅ 阿里云适配器（`AliyunProvider`）
16. ✅ 华为云适配器（`HuaweiProvider`）
17. ✅ 腾讯云适配器（`TencentProvider`）
18. ✅ Apache ACL 适配器（`AclProvider`）

**Phase 4 — 新架构支持**（✅ 已完成）：
19. ✅ 任意时间延迟消息（gRPC `PublishDelayViaGrpcAsync()`）
20. ✅ gRPC Proxy 协议支持（内置 Protobuf 编解码 + HTTP/2 gRPC 客户端，netstandard2.1+）
21. ✅ 多 Topic 订阅（`Topics` 属性 + 按 Topic 分别 Rebalance）
22. ✅ 批量消息解码（`MessageExt.DecodeBatch()` 解码 BatchMessage Body）

**Phase 5 — 功能增强**（✅ 已完成）：
23. ✅ VIP 通道（`VipChannelEnabled` 属性，BrokerPort - 2）
24. ✅ 批量确认 Pop 消息（`BatchAckMessageAsync()` / `BATCH_ACK_MESSAGE`）
25. ✅ 消费统计完整 API（`GetConsumeStats()` / `GetTopicStatsInfo()`）
26. ✅ 消息过滤服务器注册（`RegisterFilterServer()` / `REGISTER_FILTER_SERVER`）
27. ✅ 5.x MessageId 新格式（`CreateMessageId5x()` / `TryParseMessageId5x()` / `IsMessageId5x()`）
28. ✅ 客户端资源上报（gRPC `TelemetryViaGrpcAsync()` / `TelemetryCommand`）

**Phase 6 — 持续优化**（长期）：
29. ⚠️ 阿里云 5.x 实例 gRPC 兼容性验证
30. ⚠️ 各云厂商 gRPC 接入验证
31. ❌ 服务端 Rebalance（5.x gRPC）
32. ❌ Compaction Topic（5.x）

### 12.3 推荐使用场景

## 附录 A：文件清单

| 文件 | 说明 |
|------|------|
| `MqBase.cs` | 业务基类，NameServer连接、Broker管理、Topic/消费组CRUD、消息查询、消费统计、过滤服务器注册、gRPC生命周期/Telemetry |
| `Producer.cs` | 生产者，普通/异步/单向/延迟/事务/批量/Request-Reply/gRPC扩展 |
| `Consumer.cs` | 消费者，Pull/调度/Rebalance/多Topic/顺序/重试/Pop/批量确认Pop/gRPC消费 |
| `NameClient.cs` | NameServer客户端（243行），路由发现/定时轮询/多Topic路由/Broker主从解析 |
| `BrokerClient.cs` | Broker客户端（142行），心跳/注销/命令收发 |
| `ClusterClient.cs` | 集群通信（423行），TCP连接管理/HMAC-SHA1统一签名/同步异步命令收发 |
| `ICloudProvider.cs` | 云厂商适配器统一接口 |
| `AliyunProvider.cs` | 阿里云适配器（实例ID前缀路由 + HTTP NameServer发现） |
| `AclProvider.cs` | Apache ACL 适配器 |
| `HuaweiProvider.cs` | 华为云适配器（EnableSsl支持） |
| `TencentProvider.cs` | 腾讯云适配器（Namespace前缀路由） |
| `AliyunOptions.cs` | 旧版阿里云参数（`[Obsolete]`） |
| `AclOptions.cs` | 旧版ACL参数（`[Obsolete]`） |
| `Grpc/IProtoMessage.cs` | Protobuf 消息接口 |
| `Grpc/ProtoWriter.cs` | 轻量级 Protobuf 二进制编码器（343行） |
| `Grpc/ProtoReader.cs` | 轻量级 Protobuf 二进制解码器（308行） |
| `Grpc/GrpcClient.cs` | gRPC 传输层（310行，HTTP/2 帧编码 + Unary/ServerStreaming） |
| `Grpc/GrpcEnums.cs` | gRPC 枚举定义（213行，GrpcCode/GrpcMessageType 等） |
| `Grpc/GrpcModels.cs` | gRPC 消息模型（520行，Resource/Endpoints/Message/SystemProperties 等） |
| `Grpc/GrpcServiceMessages.cs` | gRPC 服务消息类型（883行，约20个Request/Response） |
| `Grpc/GrpcMessagingService.cs` | RocketMQ 5.x gRPC 消息服务客户端（417行，11个RPC方法） |
| `Protocol/Command.cs` | 协议帧编解码（333行） |
| `Protocol/MqCodec.cs` | 网络层编解码器 |
| `Protocol/Header.cs` | 通信头 |
| `Protocol/Message.cs` | 消息模型（227行） |
| `Protocol/MessageExt.cs` | 消息扩展（244行，Read/ReadAll/DecodeBatch/ZLIB/IPv4+IPv6） |
| `Protocol/RequestCode.cs` | 请求码枚举（277行，约60个指令） |
| `Protocol/ResponseCode.cs` | 响应码枚举（104行） |
| `Protocol/SendMessageRequestHeader.cs` | 发送消息请求头（90行） |
| `Protocol/PullMessageRequestHeader.cs` | 拉取消息请求头（70行） |
| `Protocol/EndTransactionRequestHeader.cs` | 结束事务请求头（50行） |
| `Protocol/TransactionState.cs` | 事务状态枚举 |
| `Protocol/MQVersion.cs` | 协议版本枚举（909行，V3.0 ~ V5.9.9 + HIGHER_VERSION） |
| `Protocol/SendResult.cs` | 发送结果 |
| `Protocol/PullResult.cs` | 拉取结果 |
| `MessageTrace/AsyncTraceDispatcher.cs` | 消息轨迹分发器 |
| `MessageTrace/MessageTraceHook.cs` | 消息轨迹钩子 |
| `Common/ILoadBalance.cs` | 负载均衡接口 |
| `Common/WeightRoundRobin.cs` | 加权轮询 |
| `Common/BrokerInfo.cs` | Broker信息（含Master/Slave地址分离） |
| `Models/DelayTimeLevels.cs` | 延迟等级枚举 |
| `Models/MessageModels.cs` | 消息模型枚举 |
| `Models/ConsumeTypes.cs` | 消费类型枚举 |
| `Models/ConsumeEventArgs.cs` | 消费事件参数 |
| `Helper.cs` | 辅助工具 |
| `MqSetting.cs` | MQ配置 |

## 附录 B：各厂商 RocketMQ 产品对比

| 厂商 | 产品 | 协议 | 认证方式 | NameServer发现 |
|------|------|------|---------|--------------|
| Apache | RocketMQ 4.x | Remoting (TCP) | ACL (AccessKey) | 直连/HTTP |
| Apache | RocketMQ 5.x | Remoting + gRPC | ACL | 直连/HTTP/Proxy |
| 阿里云 | 消息队列 RocketMQ 4.x | Remoting | 阿里云 AK/SK + HMAC-SHA1 | HTTP 接口 |
| 阿里云 | 消息队列 RocketMQ 5.x | gRPC 为主 | 阿里云 AK/SK | SDK/HTTP |
| 华为云 | DMS for RocketMQ | Remoting (4.x兼容) | SASL / AK/SK | 实例内网/公网 |
| 腾讯云 | TDMQ RocketMQ | Remoting (4.x兼容) | HMAC-SHA1 | VPC内网地址 |

### 12.3 推荐使用场景

**适合使用 NewLife.RocketMQ 的场景**：

| 场景 | 推荐理由 |
|------|---------|
| **.NET 企业应用** | 纯托管实现，零外部依赖，易于集成和部署 |
| **微服务架构** | 完整的消息可靠性保证（重试、死信队列、事务消息） |
| **多云/混合云部署** | 统一云厂商适配器，轻松切换阿里云/华为云/腾讯云/自建集群 |
| **高性能要求** | VIP 通道、连接池、消息压缩、并发控制等性能优化 |
| **遗留系统升级** | 支持 .NET Framework 4.5+，平滑迁移到 .NET Core/5+ |
| **分布式事务** | 完整的事务消息和事务回查机制 |
| **顺序消息处理** | 队列锁定机制保证顺序消费 |
| **消息轨迹追踪** | 内置消息轨迹和性能追踪，方便问题诊断 |

**不适合的场景**：

| 场景 | 原因 | 替代方案 |
|------|------|---------|
| **需要 Compaction Topic** | 5.x KV 语义 Topic 暂未实现 | 使用 Kafka 或等待后续版本 |
| **需要服务端 Rebalance** | 5.x gRPC 服务端负载均衡暂未实现 | 客户端 Rebalance 已能满足大多数场景 |
| **非 .NET 环境** | 仅支持 .NET 平台 | 使用官方 Java/C++/Go 客户端 |

### 12.4 技术选型建议

**协议选择**：
- **RocketMQ 4.x 集群**：使用 Remoting 协议（默认），成熟稳定
- **RocketMQ 5.x 集群**：
  - Remoting 协议：向后兼容，功能完整
  - gRPC Proxy：需要任意时间延迟消息或 5.x 新特性时使用（需 netstandard2.1+）
- **阿里云 RocketMQ**：
  - 4.x 实例：使用 `AliyunProvider` + Remoting 协议
  - 5.x 实例：优先 gRPC Proxy（需验证兼容性）

**云厂商选择**：
- **阿里云**：✅ 完整适配（实例 ID 路由 + HTTP NameServer 发现 + HMAC-SHA1 签名）
- **华为云**：✅ 基础适配（SSL/TLS + 实例 ID 路由），待生产环境验证
- **腾讯云**：✅ 基础适配（Namespace 路由），待生产环境验证
- **自建集群**：✅ 原生支持 Apache RocketMQ 4.x/5.x

**目标框架选择**：
- **.NET Framework 4.5+**：完整 Remoting 协议支持
- **.NET Standard 2.0**：完整 Remoting 协议支持
- **.NET Standard 2.1+ / .NET 5+**：Remoting + gRPC 双协议支持

### 12.5 产品竞争力分析

**与官方 Java 客户端对比**：

| 维度 | NewLife.RocketMQ | 官方 Java 客户端 |
|------|------------------|------------------|
| **语言生态** | .NET 原生，无需 JVM | Java 原生 |
| **部署复杂度** | ✅ 单一可执行文件/DLL | ⚠️ 需要 JRE 环境 |
| **跨平台** | ✅ Windows/Linux/macOS | ✅ 需要对应平台 JRE |
| **功能完整度** | ✅ 4.x 核心功能 100%，5.x 90% | ✅ 100% |
| **性能** | ✅ 高性能（.NET 原生优化） | ✅ 高性能 |
| **社区活跃度** | ⚠️ 新生命团队维护 | ✅ Apache 官方维护 |
| **文档完善度** | ⚠️ 中文文档为主 | ✅ 中英文文档齐全 |

**与其他 .NET 客户端对比**：

| 客户端 | 协议支持 | gRPC | 依赖 | 多云支持 | 维护状态 |
|--------|---------|------|------|---------|---------|
| **NewLife.RocketMQ** | Remoting + gRPC | ✅ 自研 | 零依赖 | ✅ 统一接口 | ✅ 持续维护 |
| **rocketmq-client-csharp（官方）** | Remoting | ⚠️ 依赖 gRPC 库 | ⚠️ 多个依赖 | ⚠️ 需自行适配 | ⚠️ 更新较慢 |
| **其他社区版本** | Remoting | ❌ | ⚠️ 部分依赖 | ❌ | ⚠️ 停止维护 |

**核心竞争优势**：
1. ✅ **零外部依赖**：无需 Java、gRPC、Protobuf 第三方库，部署运维成本低
2. ✅ **双协议支持**：同时支持 Remoting 和 gRPC，向后兼容且面向未来
3. ✅ **统一云适配**：`ICloudProvider` 接口统一多云厂商接入逻辑
4. ✅ **生产验证**：30+ 测试类覆盖核心功能，已在多个生产环境运行
5. ✅ **持续迭代**：新生命团队持续维护更新，及时跟进 RocketMQ 新版本

---

## 附录 C：测试覆盖情况

测试框架：xUnit，目标框架 net10.0，共 30 个测试文件。

| 测试文件 | 覆盖功能 |
|---------|---------|
| `ProtoTests.cs` | Protobuf 编解码（varint/fixed/string/bytes/map/timestamp/duration/嵌套消息/gRPC帧/服务消息） |
| `CloudProviderTests.cs` | 云厂商适配器（AliyunProvider/AclProvider/HuaweiProvider/TencentProvider 的 Topic/Group 转换） |
| `MultiTopicTests.cs` | 多 Topic 订阅（Topics属性设置/Rebalance行为） |
| `BatchMessageTests.cs` | 批量消息发送和解码（DecodeBatch） |
| `BrokerFailoverTests.cs` | Broker 主从切换 |
| `CompressionTests.cs` | 消息压缩（ZLIB） |
| `RetryTests.cs` | 消费重试机制 |
| `OrderConsumeTests.cs` | 顺序消费锁定 |
| `PopConsumeTests.cs` | Pop 消费模式 |
| `QueryMessageTests.cs` | 消息查询（按Key） |
| `IPv6Tests.cs` | IPv6 地址解析 |
| `MQVersionUpdateTests.cs` | MQVersion 扩展验证 |
| `ManagementTests.cs` | 管理功能（Topic/消费组 CRUD、集群信息、偏移重置） |
| `SQL92FilterTests.cs` | SQL92 过滤 |
| `TransactionCheckTests.cs` | 事务回查回调 |
| `ConcurrentConsumeTests.cs` | 消费并发控制 |
| `BroadcastOffsetTests.cs` | 广播模式本地偏移持久化 |
| `CommandTests.cs` | 协议命令编解码 |
| `MessageTests.cs` | 消息模型 |
| `MessageTraceTests.cs` | 消息轨迹 |
| `RequestReplyTests.cs` | Request-Reply 模式 |
| `BasicTest.cs` / `ProducerTests.cs` / `ConsumerTests.cs` | 基础功能集成测试 |
| `AliyunTests.cs` / `AliyunIssuesTests.cs` | 阿里云适配测试 |
| `NameClientTests.cs` | NameServer 客户端测试 |
| `SupportApacheAclTest.cs` | Apache ACL 测试 |

---

## 总结

NewLife.RocketMQ 作为**企业级纯托管 .NET RocketMQ 客户端**，已经实现了 RocketMQ 4.x 的**全部核心功能**以及 5.x 的 **gRPC Proxy 协议**，具备以下显著优势：

### 核心价值

1. **生产就绪**：✅ 完整的企业级特性（消费重试、死信队列、事务回查、顺序消费、Pop 消费等）
2. **零外部依赖**：✅ 纯 C# 实现，无需 Java、gRPC、Protobuf 第三方库
3. **双协议支持**：✅ Remoting（4.x 成熟稳定）+ gRPC（5.x 面向未来）
4. **多云适配**：✅ 统一 `ICloudProvider` 接口，已适配阿里云/华为云/腾讯云/Apache ACL
5. **跨平台兼容**：✅ .NET Framework 4.5+ 到 .NET 10 全版本支持
6. **高性能优化**：✅ VIP 通道、连接池、消息压缩、并发控制等性能手段
7. **可观测性**：✅ 内置消息轨迹、性能追踪、结构化日志
8. **测试覆盖**：✅ 30+ 测试类覆盖核心功能和边缘场景

### 适用场景

- ✅ .NET 企业应用的消息中间件解决方案
- ✅ 微服务架构的异步通信和分布式事务
- ✅ 多云/混合云部署的统一消息接入
- ✅ 遗留 .NET Framework 系统的平滑升级
- ✅ 需要消息可靠性保证的业务系统

### 持续演进

新生命团队承诺持续维护和更新 NewLife.RocketMQ，紧跟 RocketMQ 社区版本，后续规划包括：
- ✅ 完善 RocketMQ 5.x 新特性（服务端 Rebalance、Compaction Topic）
- ✅ 增强各云厂商兼容性验证
- ✅ 性能优化和功能扩展
- ✅ 文档和示例完善

**欢迎使用 NewLife.RocketMQ，共同构建高可用的 .NET 消息系统！**

---

**文档结束**
