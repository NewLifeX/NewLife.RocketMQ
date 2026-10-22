# NewLife.RocketMQ - Enterprise-Grade Fully Managed RocketMQ Client

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
🌐 [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

**An enterprise-grade, fully managed RocketMQ client** supporting `.NET Framework 4.5+` / `.NET Standard 2.0+` / `.NET Core` / `.NET 5+`.  
**Implemented entirely in C#, with zero external dependencies (no Java, gRPC or Protobuf third-party libraries required).**

---

## Product Overview

NewLife.RocketMQ is an **enterprise-grade, fully managed RocketMQ client** developed by the NewLife team and designed for the .NET ecosystem. It supports both the RocketMQ **Remoting protocol (4.x/5.x Broker)** and the **gRPC Proxy protocol (5.x Proxy)**, covering all core producer and consumer capabilities as well as enterprise features, with unified adapters for Alibaba Cloud, Huawei Cloud, Tencent Cloud and the Apache ACL authentication system.

**Core advantages**:

| Feature | Description |
|---------|-------------|
| **Dual-protocol support** | Remoting (mature and stable on 4.x) + gRPC (future-facing on 5.x), with automatic routing |
| **Zero external dependencies** | Built-in Protobuf field codec (hand-written on NewLife.Core's SpanWriter/SpanReader), no Java or gRPC runtime required |
| **Multi-cloud support** | Unified `ICloudProvider` interface with built-in adapters for Alibaba Cloud, Huawei Cloud, Tencent Cloud and Apache ACL |
| **Production ready** | Complete enterprise features: consume retry, dead-letter queue, transaction checkback, ordered consumption, Pop consumption and more |
| **Broadest framework coverage** | From .NET Framework 4.5+ to .NET 10; gRPC features are available on .NET Standard 2.1+ |
| **AOT friendly** | The net10.0 target supports NativeAOT/trimming: frame encoding/decoding, JSON serialization and message compression do not rely on runtime reflection. The repository includes an AOT smoke test and a NuGet package consumption AOT smoke test; CI validates AOT publishing and execution on a mainstream RID matrix (win/linux/osx, musl and Loongson LA64) |
| **Runtime localization** | Logs, protocol parsing exceptions, public API exceptions and configuration descriptions use English as the source text, with built-in word lists for 31 languages (132 entries), translated automatically according to `Localization.Culture` with English fallback; prefixed logs are translated before the prefix is composed, and the prefix does not participate in word-list lookup |
| **High performance** | Built on the NewLife.Net high-performance network layer: connection reuse, VIP channel, message compression and concurrency control |

---

## Installation

```powershell
# NuGet Package Manager
Install-Package NewLife.RocketMQ

# .NET CLI
dotnet add package NewLife.RocketMQ
```

```xml
<!-- PackageReference -->
<PackageReference Include="NewLife.RocketMQ" Version="3.0.*" />
```

---

## Quick Start

### Sending Messages

```csharp
using NewLife.RocketMQ;

var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    Group = "producer_group"
};
producer.Start();

// Synchronous send
var result = producer.Publish("Hello RocketMQ!");
Console.WriteLine($"Message ID: {result.MsgId}");

// Asynchronous send
await producer.PublishAsync("async message");

// Batch send
await producer.PublishBatch(new[] { "message1", "message2", "message3" });
```

### Consuming Messages

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
        Console.WriteLine($"Received message: {msg.BodyString}");
    }
    return true; // Returning true indicates successful consumption
};

consumer.Start();
```

### gRPC (5.x Proxy) Access

```csharp
// Producer: setting GrpcProxyAddress routes traffic over the 5.x gRPC path (requires netstandard2.1+)
var producer = new Producer
{
    Topic = "test_topic",
    Group = "producer_group",
    NameServerAddress = "127.0.0.1:9876",
    GrpcProxyAddress = "http://127.0.0.1:8081",
};
producer.Start();
await producer.PublishAsync("gRPC message");

// Consumer: subscription relationships are reported automatically at startup (Telemetry long connection); receiving and acknowledgement go over gRPC
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
// ... business processing ...
await consumer.AckMessageViaGrpcAsync("test_topic",
[
    .. messages.Select(m => new AckMessageEntry
    {
        ReceiptHandle = m.SystemProperties?.ReceiptHandle,
        MessageId = m.SystemProperties?.MessageId,
    }),
]);
```

> Long-polling consumption can also use `GrpcPushConsumer` directly (PUSH_CONSUMER semantics with built-in automatic Ack and retry).
>
> **5.x POP retry and dead letters**: messages that fail consumption are written by the Broker to `%RETRY%{group}_{topic}` and, according to the `popFromRetryProbability`, merged back into the original topic's pull response; the client only needs to consume the original topic. When the delivery count reaches `GrpcPushConsumer.MaxDeliveryAttempts` (default 16), the message is moved to `%DLQ%{group}`; in manual-receive scenarios, call `Consumer.ForwardToDeadLetterQueueViaGrpcAsync` (`GetGrpcDlqTopic()` returns the dead-letter topic name). The invisible-time lower bound is 10 seconds (automatically converged inside the library); smaller values are rejected by the Broker.
>
> Dead-letter messages must be processed in a single pass: `dotnet run --file scripts/DlqTool.cs -- --group <consumer group> --to <original topic>` re-publishes messages from the dead-letter queue to the specified topic and acknowledges only the successfully re-published messages. Pulling dead-letter messages advances the consume offset, and unacknowledged messages are moved by the Broker into `%RETRY%{group}_%DLQ%{group}`; therefore, read-only browsing of the dead-letter queue is not supported.
>
> **FIFO ordered messages and transactional messages**: the topic type must match the message type; create it with `CreateTopic(topic, queueNum, 0, TopicMessageType.Fifo/Transaction)`. For FIFO sends, set `Message.MessageGroup`; on the consumer side, set `GrpcPushConsumer.Orderly = true` to process serially and preserve order. For transactional messages, use `PublishTransactionViaGrpcAsync` to send the half message and `EndTransactionViaGrpcAsync` to commit or roll back (if not committed, the Broker rolls it back after a timeout).

### Delayed Messages

```csharp
// 18 preset delay levels
producer.PublishDelay("delayed message", DelayTimeLevels.s30);

// gRPC mode supports arbitrary delays (requires netstandard2.1+)
producer.GrpcProxyAddress = "http://127.0.0.1:8081";
await producer.PublishDelayViaGrpcAsync("arbitrary delay", DateTime.Now.AddMinutes(30));
```

### Transactional Messages

```csharp
var producer = new Producer
{
    Topic = "tx_topic",
    Group = "tx_group",
    NameServerAddress = "127.0.0.1:9876"
};

// Transaction checkback callback
producer.OnCheckTransaction = (msg, transactionId) =>
{
    var success = CheckLocalTransaction(transactionId);
    return success ? TransactionState.Commit : TransactionState.Rollback;
};
producer.Start();

// Send half message -> execute local transaction -> commit/rollback
var sendResult = producer.PublishTransaction("order created");
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

### Ordered Messages

```csharp
// Messages with the same key go to the same queue
var queue = producer.SelectQueue("order_123");
producer.Publish("ordered message 1", queue);
producer.Publish("ordered message 2", queue);

// Enable ordered consumption on the consumer side
consumer.OrderConsume = true;
```

### Request-Reply Pattern

```csharp
// Producer sends a request (sync/async)
var response = producer.Request("request message", timeout: 5000);
var reply = await producer.RequestAsync("async request", timeout: 5000);

// Consumer replies
consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        if (!String.IsNullOrEmpty(msg.CorrelationId))
            consumer.SendReply(msg, "processing result");
    }
    return true;
};
```

### Dual-Protocol Message Conversion

```csharp
// Convert a Remoting message to a gRPC message (requires netstandard2.1+)
var grpcMsg = message.ToGrpcMessage();

// Convert a gRPC message to an extended Remoting message / basic message
var ext = grpcMsg.ToMessageExt();   // MessageExt (includes all system properties)
var msg = grpcMsg.ToMessage();      // Message (basic fields)
```

---

## Consumer Advanced Features

### Consume Retry and Dead-Letter Queue

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    EnableRetry = true,           // Enable consume retry
    MaxReconsumeTimes = 3         // Maximum retry count; exceeded messages enter the %DLQ% dead-letter queue
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        try { ProcessMessage(msg); }
        catch { return false; } // Returning false triggers a retry
    }
    return true;
};
```

### Tag / SQL92 Filtering

```csharp
// Tag filtering
consumer.Tags = "TagA || TagB";

// SQL92 expression filtering
consumer.ExpressionType = "SQL92";
consumer.Subscription = "age > 18 AND city = 'Shanghai'";
```

### Multi-Topic Subscription

```csharp
consumer.Topics = "topic1;topic2;topic3";
```

### Pop Consumption Mode

```csharp
// Pop consumption (manual acknowledgement)
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
        await consumer.ChangeInvisibleTimeAsync(msg, 30000); // Extend the processing time
    }
}
```

### Consume Throttling / VIP Channel / Message Compression

```csharp
consumer.MaxConcurrentConsume = 10; // Process at most 10 messages concurrently

producer.VipChannelEnabled = true;  // Enable the VIP channel (BrokerPort - 2)
producer.CompressOverBytes = 4096;  // Automatically ZLIB-compress message bodies larger than 4KB
```

### HTTP Bridge (MQ -> WebAPI)

To forward consumed messages to a WebAPI, or to expose a WebAPI for external message publishing, you can combine the existing APIs; see the [HTTP Bridge Guide](Doc/HttpBridge_Guide.md).

---

## Cloud Provider Access

### Alibaba Cloud ApsaraMQ for RocketMQ

```csharp
var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "http://MQ_INST_xxx.aliyuncs.com:80",
    CloudProvider = new AliyunProvider
    {
        AccessKey = "your AccessKey",
        SecretKey = "your SecretKey",
        InstanceId = "MQ_INST_xxx"  // Optional; parsed automatically from the address
    }
};
```

### Huawei Cloud DMS for RocketMQ

```csharp
var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "Huawei Cloud instance address:9876",
    CloudProvider = new HuaweiProvider
    {
        AccessKey = "your AK",
        SecretKey = "your SK",
        InstanceId = "instance ID",
        EnableSsl = true
    }
};
```

### Tencent Cloud TDMQ RocketMQ

```csharp
var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "Tencent Cloud instance address:9876",
    CloudProvider = new TencentProvider
    {
        AccessKey = "Tencent Cloud SecretId",
        SecretKey = "Tencent Cloud SecretKey",
        Namespace = "namespace"
    }
};
```

### Apache RocketMQ ACL Authentication

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

## NativeAOT Publishing

Only the `net10.0` target supports NativeAOT/trimming (older target frameworks have no trimming concept and behave unchanged). The package already includes source-generated member accessors and metadata roots, so no extra configuration is required when consuming it:

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

Verified scenarios (measured with native executables): frame encoding/decoding, request-header property dictionaries, JSON serialization, message compression (ZLIB round-trip and decompression limits), LMQ properties, ACL signing, cloud provider adapters, dual-protocol conversion, and end-to-end send/receive (normal/object body/batch/delayed/large-message compression, consumption, offset query, ordered lock, POP) as well as the gRPC 5.x path (routing, send/receive acknowledgement, push consumption, dead-letter forwarding, FIFO ordered consumption).

CI coverage: win-x64 (including full local RocketMQ end-to-end), linux-x64, linux-arm64, win-arm64, osx-x64/osx-arm64, linux-musl-x64/arm64 are each natively published and run; Loongson linux-loongarch64 runs via QEMU user-mode emulation plus the Loongson SDK (there is currently no official NativeAOT support for that platform, so an AOT-style engineering JIT smoke test is used as the criterion).

**Custom type object bodies must be registered**: when a custom type (POCO) is used as a message body, the trimmer removes its public properties by default, and the object body is silently serialized as an empty object `{}`. Register each type used as a message body once at application startup:

```csharp
MqAot.RegisterBodyType<MyOrder>();
MqAot.RegisterBodyType<MyEvent>();
```

`MqAot.RegisterBodyType<T>()` only has trimming-preservation semantics on net10.0; it is a no-op on other targets, so multi-target projects can share the same startup code. You can also declare your own metadata roots using the same pattern as the library (`[DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MyOrder))]` + `[ModuleInitializer]`).

---

## Architecture Overview

```
MqBase (business base class)
├── Producer
└── Consumer

Communication layer
├── Remoting protocol (4.x/5.x Broker)
│   ├── ClusterClient (TCP long connection, Opaque reuse)
│   ├── NameClient (route discovery, 30s polling)
│   └── BrokerClient (heartbeat/unregister)
│
└── gRPC protocol (5.x Proxy, netstandard2.1+)
    ├── GrpcClient (HTTP/2, Unary + Server Streaming + write-only long connection)
    ├── GrpcMessagingService (11 RPC methods)
    ├── GrpcTelemetrySession (resource reporting long connection: periodic resend + reconnect on disconnect)
    ├── MessageConverter (Remoting ↔ gRPC message conversion)
    └── Field codec (hand-written Protobuf based on SpanWriter/SpanReader)

Cloud provider adapter layer
├── AliyunProvider  (Alibaba Cloud: instance ID routing + HTTP NameServer)
├── HuaweiProvider  (Huawei Cloud: SSL/TLS + instance ID routing)
├── TencentProvider (Tencent Cloud: Namespace prefix routing)
└── AclProvider     (Apache ACL: HMAC-SHA1 signing)
```

See the [Requirements Document](Doc/需求文档.md), [Feature List](Doc/功能清单.md), [Architecture Design](Doc/架构设计.md), [Competitive Analysis](Doc/竞品分析.md), [HTTP Bridge Guide](Doc/HttpBridge_Guide.md) and [Localization](Doc/多语言本地化.md).

---

## Feature Overview

### Producer

| Feature | Status | Description |
|---------|:------:|-------------|
| Sync/async/one-way send | ✅ | Publish / PublishAsync / PublishOneway |
| Batch message send | ✅ | PublishBatch, merging multiple messages into one request |
| Delayed messages | ✅ | 18 preset levels + arbitrary delays over gRPC |
| Transactional messages | ✅ | Half message + commit/rollback + checkback callback |
| Ordered messages | ✅ | Send to a specified MessageQueue |
| Request-Reply | ✅ | Synchronous/asynchronous request-reply |
| Message compression | ✅ | Automatic ZLIB above the CompressOverBytes threshold |
| Message trace | ✅ | AsyncTraceDispatcher + MessageTraceHook |

### Consumer

| Feature | Status | Description |
|---------|:------:|-------------|
| Pull consumption / consume scheduling | ✅ | Long-polling pull with automatic queue assignment |
| Clustered/broadcast consumption | ✅ | Rebalance average assignment / local offset persistence |
| Tag / SQL92 filtering | ✅ | Expression filtering |
| Multi-topic subscription | ✅ | Topics property; Rebalance per topic |
| Consume retry + dead-letter queue | ✅ | EnableRetry + MaxReconsumeTimes |
| Ordered consumption | ✅ | Queue locking (OrderConsume) |
| Pop consumption | ✅ | Pop/Ack/BatchAck/ChangeInvisibleTime |
| Consume throttling | ✅ | MaxConcurrentConsume semaphore control |

### Management and Operations

| Feature | Status | Description |
|---------|:------:|-------------|
| Topic/consumer group CRUD | ✅ | Create/update/delete |
| Message query | ✅ | By ID / by Key |
| Consume statistics / cluster information | ✅ | GetConsumeStats / GetClusterInfo |
| Offset management and reset | ✅ | Query/update/reset |

### Protocols and Compatibility

| Server version | Remoting | gRPC | Description |
|----------------|:--------:|:----:|-------------|
| RocketMQ 4.0 ~ 4.9 | ✅ | — | Fully compatible |
| RocketMQ 5.x (Broker) | ✅ | — | Remoting is backward compatible |
| RocketMQ 5.x (Proxy) | — | ✅ | Enabled via GrpcProxyAddress |
| Alibaba Cloud 4.x | ✅ | — | AliyunProvider adapter |
| Huawei Cloud DMS | ✅ | — | HuaweiProvider adapter |
| Tencent Cloud TDMQ | ✅ | — | TencentProvider adapter |

---

## Test Coverage

The xUnit test suite covers core functionality, advanced features, protocol compatibility, cloud provider adapters and E2E integration tests (gRPC integration cases are skipped automatically when no Proxy is configured); it also verifies the authentication semantics of Apache ACL 2.0 on both Remoting and gRPC (requests without credentials, from non-admin users or with wrong keys are rejected, while admins can send and receive normally). Unit tests run in parallel by test class, and `dotnet test XUnitTestRocketMQ/XUnitTest.csproj --filter "Category!=Integration"` completes in about 4 seconds; integration cases that depend on a real Broker are placed in the RocketMQ collection and run serially to avoid interference from shared topics and consumer groups; set `ROCKETMQ_TEST_VERBOSE=1` to restore framework logs when troubleshooting. CI runs as a RID matrix: unit tests and AOT smoke tests cover win-x64 / linux-x64 / linux-arm64 / Loongson LA64 (QEMU emulation, required), while win-arm64, both osx architectures and both linux-musl architectures are in the observation period; all support manual triggering.

Also included: NativeAOT smoke tests (`AotSmoke`, 33 cases, including message compression, LMQ, ACL signing, gRPC path, dead-letter forwarding and FIFO ordered consumption), NuGet package consumption AOT smoke tests (`AotPackageSmoke`, consuming the packed artifacts via PackageReference, covering package assets, source-generated serialization, compression and end-to-end send/receive), publish baseline collection (`scripts/AotBaseline.cs`, reporting size/startup overhead/throughput with threshold checks and writing to the pipeline summary in CI), AOT/JIT performance comparison benchmarks (the `Benchmark` project based on BenchmarkDotNet, including allocation statistics; the AOT side uses multi-round sampling via `AotSmoke --bench`), a dead-letter operations tool (`scripts/DlqTool.cs`, re-publishing `%DLQ%{group}` messages to a specified topic) and a gRPC stability acceptance script (`scripts/GrpcStability.cs`, run nightly in CI with one injected Proxy restart).

### Local Integration Tests (Windows)

```powershell
# One-step install and start of NameServer + Broker + Proxy (the store is cleared on every startup)
dotnet run --file scripts/RocketMqSetup.cs

$env:ROCKETMQ_NAMESERVER = "127.0.0.1:9876"
dotnet test XUnitTestRocketMQ\XUnitTest.csproj -c Release

# Stop / status
dotnet run --file scripts/RocketMqSetup.cs -- --stop
dotnet run --file scripts/RocketMqSetup.cs -- --status
```

Local startup uses low-resource mode by default: logs from the three JVMs are reduced to WARN (no longer written to `%USERPROFILE%\logs\rocketmqlogs`), the async flush interval is 5 seconds, the commitlog is 64MB, thread pools are tightened, and the store is cleared on every startup so the Broker is always in a fresh state. When troubleshooting, use `--log-level INFO` (or `$env:ROCKETMQ_TEST_LOG_LEVEL="INFO"`) to restore detailed logs (log volume increases noticeably); when gRPC is not needed, use `--no-proxy` to start one fewer JVM; to point the store at a RAM disk, use `--store-dir R:\RocketMQ\store`.

---

## Contributing

Issues and Pull Requests are welcome!

1. Fork this repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## License

This project is released under the [MIT License](LICENSE).

---

## Ecosystem Map

NewLife is a layered ecosystem of independently versioned open-source projects. The map below is shared verbatim by every NewLife repository; each project links to its repository and its primary NuGet package.

| Project | What it does | Downloads |
| --- | --- | --- |
| **Foundation** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | Logging, configuration, caching, networking, serialization, scheduling and APM tracing. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | Data middleware: single tables at the tens-of-billions scale, automatic sharding, read-write splitting, 10+ databases. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | High-performance network library: 1.77M pps / 14.5 Gbps on a single machine (TCP 1KB, 2026-10), 4M long-lived TCP connections. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | Protocol communication and RPC framework for client/server applications and IoT devices. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | Installs .NET applications as OS daemons: Windows services and Linux systemd. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | `dotnet new` project templates for console, web, API, service, data and IoT projects. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **Data & Messaging** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Redis client with microsecond latency, millions of ops/s and built-in message queues. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Pure managed RocketMQ client for Apache RocketMQ and Alibaba Cloud Message Queue. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | MQTT client and server, including Alibaba Cloud IoT connectivity. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | Hybrid database in C#: embedded or networked, relational, time-series, message queue and KV in one. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | MySql driver and helper layer built on the NewLife data stack. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web, AI & Distributed** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | Rapid admin-platform framework: permissions, SSO, OAuth server and CRUD scaffolding. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | Distributed service platform: node management, APM, configuration center, registry and release center. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | Distributed task and real-time data scheduling platform with sharding, retries and backfill. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | Unified client for 46 LLM providers plus ChatAI, a ready-to-run web chat application. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **Industrial & IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | IoT standard library defining protocol contracts and device abstractions. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | Modbus TCP/RTU/ASCII implementation on top of NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | Siemens PLC protocol on top of NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | Omron PLC protocol on top of NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | Mitsubishi Melsec PLC protocol on top of NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | OPC client for industrial data access. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | BACnet building-automation protocol stack. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | Lightweight open-source IoT platform with gateway and device management. | — |
| **Vehicle Telematics** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | JT/T 808 vehicle positioning protocol: 70+ message bodies, 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | JT/T 809 platform-to-platform protocol for transport authorities. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | JT/T 1078 in-vehicle video: RTP parsing, FLV/fMP4 remuxing and live streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **Domain Components** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | One package for Excel, Word, PPT, PDF and 15+ document formats. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | Cross-platform audio: codecs, device I/O, DSP, containers and streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | Video codecs, container formats, frame processing and streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | Unified interface over Baidu, AMap, Tencent and Tianditu with geocoding and coordinate conversion. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | IPv4 geolocation with an offline memory-mapped database and an online API fallback. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **Tools & Products** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | Developer toolbox: network, serial port, crypto, regex, Modbus and MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | Self-hosted Git source code management system. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | Installs the matching .NET runtime automatically when launched from an older host. | — |

> Badges show the primary package of each project; theme, extension and satellite packages are listed on their NuGet pages. The 30+ packages not listed here (FTP, LoRa, Office extensions, protocol drivers and more) are available under the [NewLifeX organization](https://github.com/NewLifeX) and on [newlifex.com](https://newlifex.com).

---

## Team

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

Founded in 2002, the NewLife team has spent more than two decades on the .NET and IoT ecosystem, maintains nearly 100 open-source projects (34 of them core to this map), and delivers integrated software and hardware solutions for power, logistics, healthcare, industrial control and internet industries. We live by the motto "learning has no seniority — mastery earns respect".

Open-source components have been downloaded <!--nuget-total:start-->6.48M<!--nuget-total:end--> times from NuGet (ecosystem-wide, as of 2026-10).

Website: https://newlifex.com · Open source organization: https://github.com/NewLifeX

<details>
<summary>WeChat Official Account: 智能大石头 (scan to follow)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
