# NewLife.RocketMQ - 企业级纯托管 RocketMQ 客户端

![GitHub top language](https://img.shields.io/github/languages/top/newlifex/newlife.rocketmq?logo=github)


![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.RocketMQ?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.RocketMQ?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.RocketMQ?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.RocketMQ?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.RocketMQ/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.RocketMQ/actions/workflows/test.yml)
[![locales](https://img.shields.io/badge/locales-31-2ea44f?logo=dotnet)](Doc/多语言本地化.md)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS%20%7C%20Android%20%7C%20iOS-blue)
![arch](https://img.shields.io/badge/arch-x64%20%7C%20x86%20%7C%20arm64%20%7C%20arm%20%7C%20loongarch64%20%7C%20riscv64%20%7C%20s390x-orange)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.RocketMQ?logo=github)](https://github.com/NewLifeX/NewLife.RocketMQ/graphs/contributors)

<!-- lang:start -->
🌐 [English](README.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

**纯托管企业级 RocketMQ 客户端**，支持 `.NET Framework 4.5+` / `.NET Standard 2.0+` / `.NET Core` / `.NET 5+`。  
**完全使用 C# 实现，零外部依赖（无需 Java、gRPC、Protobuf 第三方库）。**

> ℹ️ 本中文版与 [English README](README.md) 同批维护；若两者内容不一致，以英文版为对外口径。

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
| **运行时多语言** | 日志、协议解析异常、公开 API 异常与配置项描述以英文为源文，内置 31 种语言词表（132 条），按 `Localization.Culture` 自动翻译并回退英文；带前缀日志先翻译再拼前缀，前缀不参与词表查找 |
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

详见 [需求文档](Doc/需求文档.md)、[功能清单](Doc/功能清单.md)、[架构设计文档](Doc/架构设计.md)、[竞品分析](Doc/竞品分析.md)、[HTTP 桥接指南](Doc/HttpBridge_Guide.md)、[多语言本地化](Doc/多语言本地化.md)。

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

xUnit 测试套件覆盖核心功能、高级特性、协议兼容、云厂商适配、E2E 集成测试（gRPC 集成用例在未配置 Proxy 时自动跳过）；另含 Apache ACL 2.0 在 Remoting 与 gRPC 两侧的鉴权语义验证（无凭据/非管理员/错误密钥被拒，管理员可正常收发）。单元测试按测试类并行执行，`dotnet test XUnitTestRocketMQ/XUnitTest.csproj --filter "Category!=Integration"` 约 4 秒完成；依赖真实 Broker 的集成用例统一进入 RocketMQ 集合串行执行，避免共享主题与消费组互相干扰；排查问题时设置 `ROCKETMQ_TEST_VERBOSE=1` 可恢复框架日志。CI 以 RID 矩阵运行：单元测试与 AOT 冒烟覆盖 win-x64 / linux-x64 / linux-arm64 / 龙芯 LA64（QEMU 仿真，必选），win-arm64、osx 双架构与 linux-musl 双架构为观察期，全部支持手动触发。

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

## 生态版图

NewLife 生态按依赖分层，各项目独立版本、独立发版。下面的版图在所有 NewLife 仓库中逐字一致；每个项目链接到代码仓库与主 NuGet 包。

| 项目 | 一句话定位 | 下载量 |
| --- | --- | --- |
| **基础底座** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | 日志、配置、缓存、网络、序列化、定时调度与 APM 追踪。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | 数据中间件：单表百亿级、自动分表、读写分离，支持 10 多种数据库。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | 高性能网络库：单机 177 万 pps、14.5 Gbps（TCP 1KB，2026-10 实测），400 万 Tcp 长连接。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | 协议通信与 RPC 框架，面向 CS 应用与物联网设备。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | 把 .NET 应用安装为操作系统守护进程：Windows 服务与 Linux systemd。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | `dotnet new` 项目模板：控制台、Web、WebApi、服务、数据与物联网。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **数据存储与消息** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Redis 客户端：微秒级延迟、百万级吞吐，内置消息队列。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | 纯托管 RocketMQ 客户端，支持 Apache RocketMQ 与阿里云消息队列。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | MQTT 客户端与服务端，客户端支持阿里云物联网。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | C# 混合数据库：嵌入/网络双模，关系型、时序、消息队列与 KV 于一体。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | MySql 驱动与数据访问辅助层，基于 NewLife 数据栈实现。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web、AI 与分布式** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | 魔方快速开发平台：用户权限、SSO、OAuth 服务端与 CRUD 脚手架。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | 星尘分布式服务平台：节点管理、APM、配置中心、注册中心与发布中心。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | 蚂蚁调度：分布式任务与实时数据平台，支持分片、重试与回溯补算。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | 统一接入 46 家大模型服务商，附带开箱即用的 ChatAI Web 对话应用。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **工业与物联网** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | IoT 标准库，定义物联网通信协议契约与设备抽象。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | Modbus TCP/RTU/ASCII，基于 NewLife.IoT 实现。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | 西门子 PLC 协议，基于 NewLife.IoT 实现。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | 欧姆龙 PLC 协议，基于 NewLife.IoT 实现。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | 三菱 Melsec PLC 协议，基于 NewLife.IoT 实现。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | OPC 客户端，用于工业数据采集。 | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | BACnet 楼宇自控协议栈。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | 开源轻量级 IoT 平台，含网关与设备管理。 | — |
| **车联网** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | JT/T 808 车辆定位协议：70+ 消息体，覆盖 2011/2013/2019 三个版本。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | JT/T 809 部标平台互联协议，面向运输管理部门。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | JT/T 1078 车载视频：RTP 解析、FLV/fMP4 转封装与直播推送。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **领域组件** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | 一个包覆盖 Excel、Word、PPT、PDF 等 15+ 文档格式。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | 跨平台音频：编解码、设备 IO、DSP、容器与流式处理。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | 视频编解码、封装格式、帧处理与流媒体。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | 百度/高德/腾讯/天地图统一接口，支持地理编码与坐标转换。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | IPv4 归属地：离线内存映射库 + 在线 API 双源。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **工具与产品** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | 码神工具：网络、串口、加解密、正则、Modbus 与 MQTT。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | 自托管 Git 源代码管理系统。 | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | 从低版本宿主启动时，自动安装匹配的 .NET 运行时。 | — |

> 徽章取各项目主包；主题包、扩展包与卫星包见对应 NuGet 页面。未在表中列出的 30 多个包（FTP、LoRa、Office 扩展、各类协议驱动等）见 [NewLifeX 组织](https://github.com/NewLifeX) 与 [newlifex.com](https://newlifex.com)。

---

## 新生命开发团队

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

新生命团队成立于 2002 年，二十余年专注 .NET 与物联网技术生态，主导开源项目近百个（生态版图收录 34 个主干项目），为电力、物流、医疗、工控、互联网等行业提供软硬件一体化解决方案；以“学无先后达者为师”为信条。

开源组件在 NuGet 累计下载 <!--nuget-total:start-->6.48M<!--nuget-total:end--> 次（全生态口径，截至 2026-10）。

网站：https://newlifex.com · 开源组织：https://github.com/NewLifeX

<details>
<summary>微信公众号：智能大石头（扫码关注）</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>