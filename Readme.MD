# NewLife.RocketMQ - 企业级纯托管 RocketMQ 客户端

![GitHub top language](https://img.shields.io/github/languages/top/newlifex/newlife.rocketmq?logo=github)


![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.RocketMQ?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.RocketMQ?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.RocketMQ?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.RocketMQ?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.RocketMQ/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.RocketMQ/actions/workflows/test.yml)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue)
![arch](https://img.shields.io/badge/arch-x64%20%7C%20x86%20%7C%20arm64%20%7C%20arm%20%7C%20loongarch64-orange)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.RocketMQ?logo=github)](https://github.com/NewLifeX/NewLife.RocketMQ/graphs/contributors)

<!-- lang:start -->
🌐 [繁體中文](README.zh-TW.md) | [English](README.en.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

**纯托管企业级 RocketMQ 客户端**，支持 `.NET Framework 4.5+` / `.NET Standard 2.0+` / `.NET Core` / `.NET 5+`。  
**完全使用 C# 实现，零外部依赖（无需 Java、gRPC、Protobuf 第三方库）。**

---

## 产品简介

NewLife.RocketMQ 是新生命团队开发的**企业级纯托管 RocketMQ 客户端**，专为 .NET 生态设计。同时支持 RocketMQ **Remoting 协议（4.x/5.x Broker）** 和 **gRPC Proxy 协议（5.x Proxy）**，覆盖生产者、消费者全部核心功能及企业级特性，统一适配阿里云、华为云、腾讯云及 Apache ACL 认证体系。

**核心优势**：

| 特性 | 说明 |
|------|------|
| **双协议支持** | Remoting（4.x 成熟稳定）+ gRPC（5.x 面向未来），自动路由 |
| **零外部依赖** | 内置 Protobuf 字段编解码（基于 NewLife.Core 的 SpanWriter/SpanReader 手写实现），无需 Java 或 gRPC 运行时 |
| **多云适配** | 统一 `ICloudProvider` 接口，已内置阿里云/华为云/腾讯云/Apache ACL 四家适配器 |
| **生产就绪** | 消费重试、死信队列、事务回查、顺序消费、Pop 消费等企业级特性完整支持 |
| **最广框架覆盖** | .NET Framework 4.5+ 到 .NET 10，gRPC 功能在 .NET Standard 2.1+ 可用 |
| **AOT 友好** | net10.0 目标支持 NativeAOT/裁剪：帧编解码、JSON 序列化与消息压缩不依赖运行时反射，仓库内置 AOT 冒烟与 NuGet 包消费 AOT 冒烟；CI 以主流 RID 矩阵（win/linux/osx、musl 及龙芯 LA64）验证 AOT 发布与运行 |
| **高性能** | 基于 NewLife.Net 高性能网络层，连接复用、VIP 通道、消息压缩、并发控制 |

---

## 安装

```powershell
# NuGet 包管理器
Install-Package NewLife.RocketMQ

# .NET CLI
dotnet add package NewLife.RocketMQ
```

```xml
<!-- PackageReference -->
<PackageReference Include="NewLife.RocketMQ" Version="3.0.*" />
```

---

## 快速入门

### 发送消息

```csharp
using NewLife.RocketMQ;

var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    Group = "producer_group"
};
producer.Start();

// 同步发送
var result = producer.Publish("Hello RocketMQ!");
Console.WriteLine($"消息ID: {result.MsgId}");

// 异步发送
await producer.PublishAsync("异步消息");

// 批量发送
await producer.PublishBatch(new[] { "消息1", "消息2", "消息3" });
```

### 消费消息

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876"
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        Console.WriteLine($"收到消息: {msg.BodyString}");
    }
    return true; // 返回 true 表示消费成功
};

consumer.Start();
```

### gRPC（5.x Proxy）接入

```csharp
// 生产者：设置 GrpcProxyAddress 即走 5.x gRPC 链路（需 netstandard2.1+）
var producer = new Producer
{
    Topic = "test_topic",
    Group = "producer_group",
    NameServerAddress = "127.0.0.1:9876",
    GrpcProxyAddress = "http://127.0.0.1:8081",
};
producer.Start();
await producer.PublishAsync("gRPC 消息");

// 消费者：启动时自动上报订阅关系（Telemetry 长连接），接收与确认走 gRPC
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    GrpcProxyAddress = "http://127.0.0.1:8081",
};
consumer.Start();

var route = await consumer.QueryRouteViaGrpcAsync();
var messages = await consumer.ReceiveMessageViaGrpcAsync(route.MessageQueues[0], batchSize: 8);
// ……业务处理……
await consumer.AckMessageViaGrpcAsync("test_topic",
[
    .. messages.Select(m => new AckMessageEntry
    {
        ReceiptHandle = m.SystemProperties?.ReceiptHandle,
        MessageId = m.SystemProperties?.MessageId,
    }),
]);
```

> 长轮询消费也可直接用 `GrpcPushConsumer`（PUSH_CONSUMER 语义，内置自动 Ack 与重试）。
>
> **5.x POP 重试与死信**：消费失败的消息由 Broker 写入 `%RETRY%{group}_{topic}` 并按 `popFromRetryProbability` 概率并回原主题的拉取响应，客户端只需消费原主题；投递次数达到 `GrpcPushConsumer.MaxDeliveryAttempts`（默认 16）时转入 `%DLQ%{group}`，手工接收场景调用 `Consumer.ForwardToDeadLetterQueueViaGrpcAsync`（`GetGrpcDlqTopic()` 返回死信主题名）。不可见时间下限 10 秒（库内自动收敛），小于该值会被 Broker 拒绝。
>
> 死信消息需一次处理完成：`dotnet run --file scripts/DlqTool.cs -- --group <消费组> --to <原主题>` 会把死信队列中的消息重投到指定主题，仅确认重投成功的消息。死信被拉取即推进消费位点、未确认的消息会被 Broker 移入 `%RETRY%{group}_%DLQ%{group}`，因此不支持只读浏览死信队列。
>
> **FIFO 顺序消息与事务消息**：主题类型须与消息类型匹配，用 `CreateTopic(topic, queueNum, 0, TopicMessageType.Fifo/Transaction)` 创建。FIFO 发送时设置 `Message.MessageGroup`，消费时设置 `GrpcPushConsumer.Orderly = true` 以串行处理并保持顺序；事务消息用 `PublishTransactionViaGrpcAsync` 发送半消息、`EndTransactionViaGrpcAsync` 提交或回滚（不提交则超时后被 Broker 回滚）。

### 延迟消息

```csharp
// 18 级预设延迟
producer.PublishDelay("延迟消息", DelayTimeLevels.s30);

// gRPC 模式支持任意时间延迟（需 netstandard2.1+）
producer.GrpcProxyAddress = "http://127.0.0.1:8081";
await producer.PublishDelayViaGrpcAsync("任意延迟", DateTime.Now.AddMinutes(30));
```

### 事务消息

```csharp
var producer = new Producer
{
    Topic = "tx_topic",
    Group = "tx_group",
    NameServerAddress = "127.0.0.1:9876"
};

// 事务回查回调
producer.OnCheckTransaction = (msg, transactionId) =>
{
    var success = CheckLocalTransaction(transactionId);
    return success ? TransactionState.Commit : TransactionState.Rollback;
};
producer.Start();

// 发送半消息 → 执行本地事务 → 提交/回滚
var sendResult = producer.PublishTransaction("订单创建");
try
{
    ExecuteLocalTransaction(sendResult.TransactionId);
    producer.EndTransaction(sendResult, TransactionState.Commit);
}
catch
{
    producer.EndTransaction(sendResult, TransactionState.Rollback);
}
```

### 顺序消息

```csharp
// 相同 key 的消息进入同一队列
var queue = producer.SelectQueue("order_123");
producer.Publish("顺序消息1", queue);
producer.Publish("顺序消息2", queue);

// 消费端启用顺序消费
consumer.OrderConsume = true;
```

### Request-Reply 模式

```csharp
// 生产者发送请求（同步/异步）
var response = producer.Request("请求消息", timeout: 5000);
var reply = await producer.RequestAsync("异步请求", timeout: 5000);

// 消费者回复
consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        if (!String.IsNullOrEmpty(msg.CorrelationId))
            consumer.SendReply(msg, "处理结果");
    }
    return true;
};
```

### 双协议消息转换

```csharp
// Remoting 消息转 gRPC 消息（需 netstandard2.1+）
var grpcMsg = message.ToGrpcMessage();

// gRPC 消息转 Remoting 扩展消息 / 基础消息
var ext = grpcMsg.ToMessageExt();   // MessageExt（含全部系统属性）
var msg = grpcMsg.ToMessage();      // Message（基础字段）
```

---

## 消费者高级特性

### 消费重试与死信队列

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    EnableRetry = true,           // 启用消费重试
    MaxReconsumeTimes = 3         // 最大重试次数，超过进入 %DLQ% 死信队列
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        try { ProcessMessage(msg); }
        catch { return false; } // 返回 false 触发重试
    }
    return true;
};
```

### Tag / SQL92 过滤

```csharp
// Tag 过滤
consumer.Tags = "TagA || TagB";

// SQL92 表达式过滤
consumer.ExpressionType = "SQL92";
consumer.Subscription = "age > 18 AND city = 'Shanghai'";
```

### 多 Topic 订阅

```csharp
consumer.Topics = "topic1;topic2;topic3";
```

### Pop 消费模式

```csharp
// Pop 消费（手动确认）
var messages = await consumer.PopMessageAsync(timeout: 10000);
foreach (var msg in messages)
{
    try
    {
        ProcessMessage(msg);
        await consumer.AckMessageAsync(msg);
    }
    catch
    {
        await consumer.ChangeInvisibleTimeAsync(msg, 30000); // 延长处理时间
    }
}
```

### 消费限流 / VIP 通道 / 消息压缩

```csharp
consumer.MaxConcurrentConsume = 10; // 最多同时处理 10 条消息

producer.VipChannelEnabled = true;  // 启用 VIP 通道（BrokerPort - 2）
producer.CompressOverBytes = 4096;  // 消息体超过 4KB 自动 ZLIB 压缩
```

### HTTP 桥接（MQ ↔ WebAPI）

消费消息后转发给 WebAPI、或提供 WebAPI 供外部发布消息，用现有 API 即可组合实现，写法见 [HTTP 桥接指南](Doc/HttpBridge_Guide.md)。

---

## 云厂商接入

### 阿里云消息队列 RocketMQ

```csharp
var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "http://MQ_INST_xxx.aliyuncs.com:80",
    CloudProvider = new AliyunProvider
    {
        AccessKey = "你的AccessKey",
        SecretKey = "你的SecretKey",
        InstanceId = "MQ_INST_xxx"  // 可选，自动从地址解析
    }
};
```

### 华为云 DMS for RocketMQ

```csharp
var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "华为云实例地址:9876",
    CloudProvider = new HuaweiProvider
    {
        AccessKey = "你的AK",
        SecretKey = "你的SK",
        InstanceId = "实例ID",
        EnableSsl = true
    }
};
```

### 腾讯云 TDMQ RocketMQ

```csharp
var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "腾讯云实例地址:9876",
    CloudProvider = new TencentProvider
    {
        AccessKey = "腾讯云SecretId",
        SecretKey = "腾讯云SecretKey",
        Namespace = "命名空间"
    }
};
```

### Apache RocketMQ ACL 认证

```csharp
var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    CloudProvider = new AclProvider
    {
        AccessKey = "RocketMQ AccessKey",
        SecretKey = "RocketMQ SecretKey"
    }
};
```

---

## NativeAOT 发布

仅 `net10.0` 目标支持 NativeAOT/裁剪（老目标框架无裁剪概念，行为不变）。包内已带源生成成员访问器与元数据根，接入时无需额外配置：

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

已验证场景（原生可执行文件实测）：帧编解码、请求头属性字典、JSON 序列化、消息压缩（ZLIB 往返与解压上限）、LMQ 属性、ACL 签名、云厂商适配、双协议转换，以及端到端收发（普通/对象体/批量/延迟/大消息压缩、消费、偏移查询、顺序锁、POP）与 gRPC 5.x 链路（路由、收发确认、推送消费、死信转发、FIFO 顺序消费）。

CI 覆盖：win-x64（含本地 RocketMQ 全量端到端）、linux-x64、linux-arm64、win-arm64、osx-x64/osx-arm64、linux-musl-x64/arm64 逐一原生发布运行；龙芯 linux-loongarch64 经 QEMU 用户态仿真 + 龙芯 SDK 运行（官方暂无该平台 NativeAOT 支持，以 AOT 姿态工程 JIT 冒烟为准）。

**自定义类型对象体需登记**：以自定义类型（POCO）作为消息体时，裁剪器默认会移除其公开属性，对象体将被静默序列化为空对象 `{}`。在应用启动处为每个用作消息体的类型调用一次登记即可：

```csharp
MqAot.RegisterBodyType<MyOrder>();
MqAot.RegisterBodyType<MyEvent>();
```

`MqAot.RegisterBodyType<T>()` 仅 net10.0 带裁剪保留语义，其余目标为空实现，多目标工程可共用同一份启动代码；也可按库内同套路自行声明元数据根（`[DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MyOrder))]` + `[ModuleInitializer]`）。

---

## 架构总览

```
MqBase (业务基类)
├── Producer (生产者)
└── Consumer (消费者)

通信层
├── Remoting 协议（4.x/5.x Broker）
│   ├── ClusterClient (TCP 长连接，Opaque 复用)
│   ├── NameClient (路由发现，30s 轮询)
│   └── BrokerClient (心跳/注销)
│
└── gRPC 协议（5.x Proxy，netstandard2.1+）
    ├── GrpcClient (HTTP/2，Unary + Server Streaming + 只写长连接)
    ├── GrpcMessagingService (11 个 RPC 方法)
    ├── GrpcTelemetrySession (资源上报长连接：周期重发 + 断线重连)
    ├── MessageConverter (Remoting ↔ gRPC 消息互转)
    └── 字段编解码 (基于 SpanWriter/SpanReader 手写 Protobuf)

云厂商适配层
├── AliyunProvider  (阿里云：实例ID路由 + HTTP NameServer)
├── HuaweiProvider  (华为云：SSL/TLS + 实例ID路由)
├── TencentProvider (腾讯云：Namespace 前缀路由)
└── AclProvider     (Apache ACL：HMAC-SHA1 签名)
```

详见 [需求文档](Doc/需求文档.md)、[功能清单](Doc/功能清单.md)、[架构设计文档](Doc/架构设计.md)、[竞品分析](Doc/竞品分析.md)、[HTTP 桥接指南](Doc/HttpBridge_Guide.md)。

---

## 功能特性一览

### 生产者

| 功能 | 状态 | 说明 |
|------|:----:|------|
| 同步/异步/单向发送 | ✅ | Publish / PublishAsync / PublishOneway |
| 批量消息发送 | ✅ | PublishBatch，合并多条消息为一个请求 |
| 延迟消息 | ✅ | 18 级预设 + gRPC 任意时间延迟 |
| 事务消息 | ✅ | 半消息 + 提交/回滚 + 回查回调 |
| 顺序消息 | ✅ | 指定 MessageQueue 发送 |
| Request-Reply | ✅ | 同步/异步请求回复 |
| 消息压缩 | ✅ | CompressOverBytes 阈值自动 ZLIB |
| 消息轨迹 | ✅ | AsyncTraceDispatcher + MessageTraceHook |

### 消费者

| 功能 | 状态 | 说明 |
|------|:----:|------|
| Pull 消费 / 消费调度 | ✅ | 长轮询拉取，自动分配队列 |
| 集群消费 / 广播消费 | ✅ | Rebalance 平均分配 / 本地偏移持久化 |
| Tag / SQL92 过滤 | ✅ | 表达式过滤 |
| 多 Topic 订阅 | ✅ | Topics 属性，按 Topic 分别 Rebalance |
| 消费重试 + 死信队列 | ✅ | EnableRetry + MaxReconsumeTimes |
| 顺序消费 | ✅ | 队列锁定（OrderConsume） |
| Pop 消费 | ✅ | Pop/Ack/BatchAck/ChangeInvisibleTime |
| 消费限流 | ✅ | MaxConcurrentConsume 信号量控制 |

### 管理与运维

| 功能 | 状态 | 说明 |
|------|:----:|------|
| Topic/消费组 CRUD | ✅ | 创建/更新/删除 |
| 消息查询 | ✅ | 按 ID / 按 Key |
| 消费统计 / 集群信息 | ✅ | GetConsumeStats / GetClusterInfo |
| 偏移量管理与重置 | ✅ | 查询/更新/重置 |

### 协议与兼容性

| 服务端版本 | Remoting | gRPC | 说明 |
|-----------|:--------:|:----:|------|
| RocketMQ 4.0 ~ 4.9 | ✅ | — | 完全兼容 |
| RocketMQ 5.x（Broker） | ✅ | — | Remoting 向后兼容 |
| RocketMQ 5.x（Proxy） | — | ✅ | 通过 GrpcProxyAddress 启用 |
| 阿里云 4.x | ✅ | — | AliyunProvider 适配 |
| 华为云 DMS | ✅ | — | HuaweiProvider 适配 |
| 腾讯云 TDMQ | ✅ | — | TencentProvider 适配 |

---

## 测试覆盖

86 个测试文件，655 个测试用例（xUnit），覆盖核心功能、高级特性、协议兼容、云厂商适配、E2E 集成测试（gRPC 集成用例在未配置 Proxy 时自动跳过）；另含 Apache ACL 2.0 在 Remoting 与 gRPC 两侧的鉴权语义验证（无凭据/非管理员/错误密钥被拒，管理员可正常收发）。单元测试按测试类并行执行，`dotnet test XUnitTestRocketMQ/XUnitTest.csproj --filter "Category!=Integration"` 约 4 秒完成；依赖真实 Broker 的集成用例统一进入 RocketMQ 集合串行执行，避免共享主题与消费组互相干扰；排查问题时设置 `ROCKETMQ_TEST_VERBOSE=1` 可恢复框架日志。CI 以 RID 矩阵运行：单元测试与 AOT 冒烟覆盖 win-x64 / linux-x64 / linux-arm64 / 龙芯 LA64（QEMU 仿真，必选），win-arm64、osx 双架构与 linux-musl 双架构为观察期，全部支持手动触发。

另有 NativeAOT 冒烟（`AotSmoke`，33 个用例，含消息压缩、LMQ、ACL 签名、gRPC 链路、死信转发与 FIFO 顺序消费）、NuGet 包消费 AOT 冒烟（`AotPackageSmoke`，以 PackageReference 消费打包产物，覆盖包资产、源生成序列化、压缩与端到端收发）、发布基线采集（`scripts/AotBaseline.cs`，输出体积/启动开销/吞吐并支持阈值判定，CI 写入流水线摘要）、AOT/JIT 性能对照基准（`Benchmark` 工程基于 BenchmarkDotNet，含分配统计；AOT 侧由 `AotSmoke --bench` 多轮采样）、死信运维工具（`scripts/DlqTool.cs`，把 `%DLQ%{group}` 消息重投到指定主题）与 gRPC 长稳验收脚本（`scripts/GrpcStability.cs`，CI 夜间运行并注入一次 Proxy 重启）。

### 本地集成测试（Windows）

```powershell
# 一键安装并启动 NameServer + Broker + Proxy（每次启动清空 store）
dotnet run --file scripts/RocketMqSetup.cs

$env:ROCKETMQ_NAMESERVER = "127.0.0.1:9876"
dotnet test XUnitTestRocketMQ\XUnitTest.csproj -c Release

# 停止 / 状态
dotnet run --file scripts/RocketMqSetup.cs -- --stop
dotnet run --file scripts/RocketMqSetup.cs -- --status
```

本地启动默认使用低资源模式：三个 JVM 的日志降到 WARN（不再写 `%USERPROFILE%\logs\rocketmqlogs`）、异步刷盘间隔 5 秒、commitlog 64MB、线程池收紧，并且每次启动都会清空 store，Broker 始终处于全新状态。排查问题时用 `--log-level INFO`（或 `$env:ROCKETMQ_TEST_LOG_LEVEL="INFO"`）恢复详细日志（日志量会明显增大）；不需要 gRPC 时用 `--no-proxy` 少起一个 JVM；需要把 store 指到 RAM 盘时用 `--store-dir R:\RocketMQ\store`。

---

## 参与贡献

欢迎提交 Issue 和 Pull Request！

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 提交 Pull Request

---

## 许可协议

本项目采用 [MIT License](LICENSE) 开源协议。

---

## 新生命项目矩阵

各项目默认支持 net10.0/net9.0/netstandard2.1/netstandard2.0/net4.62/net4.5

|                               项目                               | 年份  | 说明                                                                                        |
| :--------------------------------------------------------------: | :---: | ------------------------------------------------------------------------------------------- |
|                             基础组件                             |       | 支撑其它中间件以及产品项目                                                                  |
|          [NewLife.Core](https://github.com/NewLifeX/X)           | 2002  | 核心库，日志、配置、缓存、网络、序列化、APM性能追踪                                         |
|    [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode)    | 2005  | 大数据中间件，单表百亿级，MySql/SQLite/SqlServer/Oracle/PostgreSql/达梦，自动分表，读写分离 |
|      [NewLife.Net](https://github.com/NewLifeX/NewLife.Net)      | 2005  | 网络库，单机千万级吞吐率（2266万tps），单机百万级连接（400万Tcp长连接）                     |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | 2011  | 协议通信库，提供CS应用通信框架，支持Http/RPC通信框架，高吞吐，物联网设备低开销易接入        |
|     [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube)     | 2010  | 魔方快速开发平台，集成了用户权限、SSO登录、OAuth服务端等，单表100亿级项目验证               |
|    [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent)    | 2008  | 服务管理组件，把应用安装成为操作系统守护进程，Windows服务、Linux的Systemd                   |
|     [NewLife.Zero](https://github.com/NewLifeX/NewLife.Zero)     | 2020  | Zero零代脚手架，基于NewLife组件生态的项目模板NewLife.Templates，Web、WebApi、Service        |
|                              中间件                              |       | 对接知名中间件平台                                                                          |
|    [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis)    | 2017  | Redis客户端，微秒级延迟，百万级吞吐，丰富的消息队列，百亿级数据量项目验证                   |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | 2018  | RocketMQ纯托管客户端，支持Apache RocketMQ和阿里云消息队列，十亿级项目验证                   |
|     [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT)     | 2019  | 物联网消息协议，MqttClient/MqttServer，客户端支持阿里云物联网                               |
|      [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT)      | 2022  | IoT标准库，定义物联网领域的各种通信协议标准规范                                             |
|   [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus)   | 2022  | ModbusTcp/ModbusRTU/ModbusASCII，基于IoT标准库实现，支持ZeroIoT平台和IoTEdge网关            |
|  [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens)  | 2022  | 西门子PLC协议，基于IoT标准库实现，支持IoT平台和IoTEdge                                      |
|      [NewLife.Map](https://github.com/NewLifeX/NewLife.Map)      | 2022  | 地图组件库，封装百度地图、高德地图、腾讯地图、天地图                                        |
|    [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio)    | 2023  | 音频编解码库，PCM/ADPCMA/G711A/G722U/WAV/AAC                                                |
|                             产品平台                             |       | 产品平台级，编译部署即用，个性化自定义                                                      |
|         [Stardust](https://github.com/NewLifeX/Stardust)         | 2018  | 星尘，分布式服务平台，节点管理、APM监控中心、配置中心、注册中心、发布中心                   |
|           [AntJob](https://github.com/NewLifeX/AntJob)           | 2019  | 蚂蚁调度，分布式大数据计算平台（实时/离线），蚂蚁搬家分片思想，万亿级数据量项目验证         |
|      [NewLife.ERP](https://github.com/NewLifeX/NewLife.ERP)      | 2021  | 企业ERP，产品管理、客户管理、销售管理、供应商管理                                           |
|         [CrazyCoder](https://github.com/NewLifeX/XCoder)         | 2006  | 码神工具，众多开发者工具，网络、串口、加解密、正则表达式、Modbus、MQTT                      |
|           [EasyIO](https://github.com/NewLifeX/EasyIO)           | 2023  | 简易文件存储，支持分布式系统中文件集中存储                                                  |
|           [XProxy](https://github.com/NewLifeX/XProxy)           | 2005  | 产品级反向代理，NAT代理、Http代理                                                           |
|        [HttpMeter](https://github.com/NewLifeX/HttpMeter)        | 2022  | Http压力测试工具                                                                            |
|         [GitCandy](https://github.com/NewLifeX/GitCandy)         | 2015  | Git源代码管理系统                                                                           |
|          [SmartOS](https://github.com/NewLifeX/SmartOS)          | 2014  | 嵌入式操作系统，完全独立自主，支持ARM Cortex-M芯片架构                                      |
|          [SmartA2](https://github.com/NewLifeX/SmartA2)          | 2019  | 嵌入式工业计算机，物联网边缘网关，高性能.NET8主机，应用于工业、农业、交通、医疗             |
|                          FIoT物联网平台                          | 2020  | 物联网整体解决方案，建筑、环保、农业，软硬件及大数据分析一体化，单机十万级点位项目验证      |
|                        UWB高精度室内定位                         | 2020  | 厘米级（10~20cm）高精度室内定位，软硬件一体化，与其它系统联动，大型展厅项目验证             |

---

## 新生命开发团队

![XCode](https://newlifex.com/logo.png)  

新生命团队（NewLife）成立于2002年，是新时代物联网行业解决方案提供者，致力于提供软硬件应用方案咨询、系统架构规划与开发服务。  

团队主导的80多个开源项目已被广泛应用于各行业，Nuget累计下载量高达**400余万次**。  

团队开发的大数据中间件 **NewLife.XCode**、蚂蚁调度计算平台 **AntJob**、星尘分布式平台 **Stardust**、缓存队列组件 **NewLife.Redis** 以及物联网平台 **FIoT**，均成功应用于电力、高校、互联网、电信、交通、物流、工控、医疗、文博等行业，为客户提供了大量先进、可靠、安全、高质量、易扩展的产品和系统集成服务。  

我们将不断通过服务的持续改进，成为客户长期信赖的合作伙伴，通过不断的创新和发展，成为国内优秀的 IoT 服务供应商。  

**新生命团队始于2002年，部分开源项目具有20年以上漫长历史，源码库保留有2010年以来所有修改记录**  

- 网站：<https://newlifex.com>  
- 开源：<https://github.com/newlifex>  
- QQ群：1600800 / 1600838  
- 微信公众号：

![智能大石头](https://newlifex.com/stone.jpg)