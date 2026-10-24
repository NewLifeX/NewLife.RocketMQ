# NewLife.RocketMQ - 企業級純受控 RocketMQ 用戶端

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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> 本翻譯可能落後，以 [English README](README.md) 為準。

**純受控企業級 RocketMQ 用戶端**，支援 `.NET Framework 4.5+` / `.NET Standard 2.0+` / `.NET Core` / `.NET 5+`。  
**完全使用 C# 實作，零外部相依（無需 Java、gRPC、Protobuf 第三方函式庫）。**

---

## 產品簡介

NewLife.RocketMQ 是新生命團隊開發的**企業級純受控 RocketMQ 用戶端**，專為 .NET 生態設計。同時支援 RocketMQ **Remoting 協定（4.x/5.x Broker）** 與 **gRPC Proxy 協定（5.x Proxy）**，涵蓋生產者、消費者全部核心功能及企業級特性，統一適配阿里雲、華為雲、騰訊雲及 Apache ACL 認證體系。

**核心優勢**：

| 特性 | 說明 |
|------|------|
| **雙協定支援** | Remoting（4.x 成熟穩定）+ gRPC（5.x 面向未來），自動路由 |
| **零外部相依** | 內建 Protobuf 欄位編解碼（基於 NewLife.Core 的 SpanWriter/SpanReader 手寫實作），無需 Java 或 gRPC 執行階段 |
| **多雲適配** | 統一 `ICloudProvider` 介面，已內建阿里雲/華為雲/騰訊雲/Apache ACL 四家適配器 |
| **生產就緒** | 消費重試、死信佇列、交易回查、順序消費、Pop 消費等企業級特性完整支援 |
| **事件匯流排** | 內建基於 Core `IEventBus` 契約的事件匯流排：事件經 Producer/Consumer 跨行程發佈與訂閱，失敗自動回投重試/死信 |
| **最廣框架涵蓋** | .NET Framework 4.5+ 到 .NET 10，gRPC 功能在 .NET Standard 2.1+ 可用 |
| **AOT 友善** | net10.0 目標支援 NativeAOT/裁剪：訊框編解碼、JSON 序列化與訊息壓縮不依賴執行階段反射，儲存庫內建 AOT 冒煙與 NuGet 套件消費 AOT 冒煙；CI 以主流 RID 矩陣（win/linux/osx、musl 及龍芯 LA64）驗證 AOT 發佈與執行 |
| **執行階段多語言** | 日誌、協定解析例外、公開 API 例外與設定項描述以英文為源文，內建 31 種語言詞表，按 `Localization.Culture` 自動翻譯並回退英文；帶前綴日誌先翻譯再拼前綴，前綴不參與詞表查找 |
| **高效能** | 基於 NewLife.Net 高效能網路層，連線複用、VIP 通道、訊息壓縮、並行控制 |

---

## 安裝

```powershell
# NuGet 套件管理員
Install-Package NewLife.RocketMQ

# .NET CLI
dotnet add package NewLife.RocketMQ
```

```xml
<!-- PackageReference -->
<PackageReference Include="NewLife.RocketMQ" Version="4.0.*" />
```

---

## 快速入門

### 傳送訊息

```csharp
using NewLife.RocketMQ;

var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    Group = "producer_group"
};
producer.Start();

// 同步傳送
var result = producer.Publish("Hello RocketMQ!");
Console.WriteLine($"消息ID: {result.MsgId}");

// 非同步傳送
await producer.PublishAsync("异步消息");

// 批次傳送
await producer.PublishBatch(new[] { "消息1", "消息2", "消息3" });
```

### 消費訊息

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
    return true; // 回傳 true 表示消費成功
};

consumer.Start();
```

### gRPC（5.x Proxy）接入

```csharp
// 生產者：設定 GrpcProxyAddress 即走 5.x gRPC 鏈路（需 netstandard2.1+）
var producer = new Producer
{
    Topic = "test_topic",
    Group = "producer_group",
    NameServerAddress = "127.0.0.1:9876",
    GrpcProxyAddress = "http://127.0.0.1:8081",
};
producer.Start();
await producer.PublishAsync("gRPC message");

// 消費者：啟動時自動上報訂閱關係（Telemetry 長連線），接收與確認走 gRPC
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
// ……業務處理……
await consumer.AckMessageViaGrpcAsync("test_topic",
[
    .. messages.Select(m => new AckMessageEntry
    {
        ReceiptHandle = m.SystemProperties?.ReceiptHandle,
        MessageId = m.SystemProperties?.MessageId,
    }),
]);
```

> 長輪詢消費也可直接用 `GrpcPushConsumer`（PUSH_CONSUMER 語意，內建自動 Ack 與重試）。
>
> **5.x POP 重試與死信**：消費失敗的訊息由 Broker 寫入 `%RETRY%{group}_{topic}` 並按 `popFromRetryProbability` 機率併回原主題的拉取回應，用戶端只需消費原主題；投遞次數達到 `GrpcPushConsumer.MaxDeliveryAttempts`（預設 16）時轉入 `%DLQ%{group}`，手動接收場景呼叫 `Consumer.ForwardToDeadLetterQueueViaGrpcAsync`（`GetGrpcDlqTopic()` 傳回死信主題名稱）。不可見時間下限 10 秒（函式庫內自動收斂），小於該值會被 Broker 拒絕。
>
> 死信訊息需一次處理完成：`dotnet run --file scripts/DlqTool.cs -- --group <消費組> --to <原主題>` 會把死信佇列中的訊息重投到指定主題，僅確認重投成功的訊息。死信被拉取即推進消費位點、未確認的訊息會被 Broker 移入 `%RETRY%{group}_%DLQ%{group}`，因此不支援唯讀瀏覽死信佇列。
>
> **FIFO 順序訊息與交易訊息**：主題類型須與訊息類型匹配，用 `CreateTopic(topic, queueNum, 0, TopicMessageType.Fifo/Transaction)` 建立。FIFO 傳送時設定 `Message.MessageGroup`，消費時設定 `GrpcPushConsumer.Orderly = true` 以串行處理並保持順序；交易訊息用 `PublishTransactionViaGrpcAsync` 傳送半訊息、`EndTransactionViaGrpcAsync` 提交或回滾（不提交則逾時後被 Broker 回滾）。

### 延遲訊息

```csharp
// 18 級預設延遲
producer.PublishDelay("延迟消息", DelayTimeLevels.s30);

// gRPC 模式支援任意時間延遲（需 netstandard2.1+）
producer.GrpcProxyAddress = "http://127.0.0.1:8081";
await producer.PublishDelayViaGrpcAsync("任意延迟", DateTime.Now.AddMinutes(30));
```

### 交易訊息

```csharp
var producer = new Producer
{
    Topic = "tx_topic",
    Group = "tx_group",
    NameServerAddress = "127.0.0.1:9876"
};

// 交易回查回呼
producer.OnCheckTransaction = (msg, transactionId) =>
{
    var success = CheckLocalTransaction(transactionId);
    return success ? TransactionState.Commit : TransactionState.Rollback;
};
producer.Start();

// 傳送半訊息 → 執行本機交易 → 提交/回滾
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

### 順序訊息

```csharp
// 相同 key 的訊息進入同一佇列
var queue = producer.SelectQueue("order_123");
producer.Publish("顺序消息1", queue);
producer.Publish("顺序消息2", queue);

// 消費端啟用順序消費
consumer.OrderConsume = true;
```

### Request-Reply 模式

```csharp
// 生產者傳送請求（同步/非同步）
var response = producer.Request("请求消息", timeout: 5000);
var reply = await producer.RequestAsync("异步请求", timeout: 5000);

// 消費者回覆
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

### 雙協定訊息轉換

```csharp
// Remoting 訊息轉 gRPC 訊息（需 netstandard2.1+）
var grpcMsg = message.ToGrpcMessage();

// gRPC 訊息轉 Remoting 擴充訊息 / 基礎訊息
var ext = grpcMsg.ToMessageExt();   // MessageExt（含全部系統屬性）
var msg = grpcMsg.ToMessage();      // Message（基礎欄位）
```

---

### 事件匯流排（Event Bus）

```csharp
using NewLife.Messaging;
using NewLife.RocketMQ;

// 事件主題可直接使用 order.created，內部自動轉換為 RocketMQ 合法名稱 order_created
using var bus = new RocketMqEventBus<OrderPaidEvent>("127.0.0.1:9876", "order.created", "order_service");

// 訂閱：處理常式拋出例外時回投重試，超過上限進入死信佇列
await bus.SubscribeAsync((OrderPaidEvent e, IEventContext ctx) =>
{
    Console.WriteLine($"收到付款成功事件：{e.OrderId}");
    return Task.CompletedTask;
}, "billing");

// 發佈：經 Broker 投遞，本機訂閱者同樣透過消費組收到
await bus.PublishAsync(new OrderPaidEvent { OrderId = 1001 });

// 接入事件中樞：工廠按主題建立並快取匯流排
var hub = new EventHub<OrderPaidEvent>
{
    Factory = new RocketMqEventBusFactory("127.0.0.1:9876"),
};
```

> 消費組預設取本機識別，每個節點各成一組，因此每個節點都能收到事件；需要「整組只處理一次」時，向工廠傳入相同的 clientId。處理常式應保持冪等（同批次部分失敗會整批重投）。

---

## 消費者進階特性

### 消費重試與死信佇列

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    EnableRetry = true,           // 啟用消費重試
    MaxReconsumeTimes = 3         // 最大重試次數，超過進入 %DLQ% 死信佇列
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        try { ProcessMessage(msg); }
        catch { return false; } // 回傳 false 觸發重試
    }
    return true;
};
```

### Tag / SQL92 過濾

```csharp
// Tag 過濾
consumer.Tags = "TagA || TagB";

// SQL92 表達式過濾
consumer.ExpressionType = "SQL92";
consumer.Subscription = "age > 18 AND city = 'Shanghai'";
```

### 多 Topic 訂閱

```csharp
consumer.Topics = "topic1;topic2;topic3";
```

### Pop 消費模式

```csharp
// Pop 消費（手動確認）
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
        await consumer.ChangeInvisibleTimeAsync(msg, 30000); // 延長處理時間
    }
}
```

### 消費限流 / VIP 通道 / 訊息壓縮

```csharp
consumer.MaxConcurrentConsume = 10; // 最多同時處理 10 條訊息

producer.VipChannelEnabled = true;  // 啟用 VIP 通道（BrokerPort - 2）
producer.CompressOverBytes = 4096;  // 訊息體超過 4KB 自動 ZLIB 壓縮
```

### HTTP 橋接（MQ ↔ WebAPI）

消費訊息後轉發給 WebAPI、或提供 WebAPI 供外部發佈訊息，用現有 API 即可組合實現，寫法見 [HTTP 橋接指南](Doc/HttpBridge_Guide.md)。

---

## 雲廠商接入

### 阿里雲訊息佇列 RocketMQ

```csharp
var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "http://MQ_INST_xxx.aliyuncs.com:80",
    CloudProvider = new AliyunProvider
    {
        AccessKey = "你的AccessKey",
        SecretKey = "你的SecretKey",
        InstanceId = "MQ_INST_xxx"  // 可選，自動從位址解析
    }
};
```

### 華為雲 DMS for RocketMQ

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

### 騰訊雲 TDMQ RocketMQ

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

### Apache RocketMQ ACL 認證

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

## NativeAOT 發佈

僅 `net10.0` 目標支援 NativeAOT/裁剪（舊目標框架無裁剪概念，行為不變）。套件內已帶源產生器成員存取子與中繼資料根，接入時無需額外設定：

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

已驗證場景（原生可執行檔實測）：訊框編解碼、請求標頭屬性字典、JSON 序列化、訊息壓縮（ZLIB 往返與解壓上限）、LMQ 屬性、ACL 簽名、雲廠商適配、雙協定轉換，以及端對端收發（一般/物件體/批次/延遲/大訊息壓縮、消費、位移查詢、順序鎖、POP）與 gRPC 5.x 鏈路（路由、收發確認、推送消費、死信轉發、FIFO 順序消費）。

CI 涵蓋：win-x64（含本機 RocketMQ 全量端對端）、linux-x64、linux-arm64、win-arm64、osx-x64/osx-arm64、linux-musl-x64/arm64 逐一原生發佈執行；龍芯 linux-loongarch64 經 QEMU 使用者態模擬 + 龍芯 SDK 執行（官方暫無該平台 NativeAOT 支援，以 AOT 姿態工程 JIT 冒煙為準）。

**自訂型別物件體需登記**：以自訂型別（POCO）作為訊息體時，裁剪器預設會移除其公開屬性，物件體將被靜默序列化為空物件 `{}`。在應用程式啟動處為每個用作訊息體的型別呼叫一次登記即可：

```csharp
MqAot.RegisterBodyType<MyOrder>();
MqAot.RegisterBodyType<MyEvent>();
```

`MqAot.RegisterBodyType<T>()` 僅 net10.0 帶裁剪保留語意，其餘目標為空實作，多目標工程可共用同一份啟動程式碼；也可按函式庫內同套路自行宣告中繼資料根（`[DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MyOrder))]` + `[ModuleInitializer]`）。

---

## 架構總覽

```
MqBase (業務基底類別)
├── Producer (生產者)
└── Consumer (消費者)

通訊層
├── Remoting 協定（4.x/5.x Broker）
│   ├── ClusterClient (TCP 長連線，Opaque 複用)
│   ├── NameClient (路由探索，30s 輪詢)
│   └── BrokerClient (心跳/註銷)
│
└── gRPC 協定（5.x Proxy，netstandard2.1+）
    ├── GrpcClient (HTTP/2，Unary + Server Streaming + 唯寫長連線)
    ├── GrpcMessagingService (11 個 RPC 方法)
    ├── GrpcTelemetrySession (資源上報長連線：週期重送 + 斷線重連)
    ├── MessageConverter (Remoting ↔ gRPC 訊息互轉)
    └── 欄位編解碼 (基於 SpanWriter/SpanReader 手寫 Protobuf)

雲廠商適配層
├── AliyunProvider  (阿里雲：實例ID路由 + HTTP NameServer)
├── HuaweiProvider  (華為雲：SSL/TLS + 實例ID路由)
├── TencentProvider (騰訊雲：Namespace 前綴路由)
└── AclProvider     (Apache ACL：HMAC-SHA1 簽名)
```

詳見 [需求文件](Doc/需求文档.md)、[功能清單](Doc/功能清单.md)、[架構設計文件](Doc/架构设计.md)、[競品分析](Doc/竞品分析.md)、[HTTP 橋接指南](Doc/HttpBridge_Guide.md)、[多語言在地化](Doc/多语言本地化.md)。

---

## 功能特性一覽

### 生產者

| 功能 | 狀態 | 說明 |
|------|:----:|------|
| 同步/非同步/單向傳送 | ✅ | Publish / PublishAsync / PublishOneway |
| 批次訊息傳送 | ✅ | PublishBatch，合併多條訊息為一個請求 |
| 延遲訊息 | ✅ | 18 級預設 + gRPC 任意時間延遲 |
| 交易訊息 | ✅ | 半訊息 + 提交/回滾 + 回查回呼 |
| 順序訊息 | ✅ | 指定 MessageQueue 傳送 |
| Request-Reply | ✅ | 同步/非同步請求回覆 |
| 訊息壓縮 | ✅ | CompressOverBytes 閾值自動 ZLIB |
| 訊息軌跡 | ✅ | AsyncTraceDispatcher + MessageTraceHook |

### 消費者

| 功能 | 狀態 | 說明 |
|------|:----:|------|
| Pull 消費 / 消費調度 | ✅ | 長輪詢拉取，自動分配佇列 |
| 叢集消費 / 廣播消費 | ✅ | Rebalance 平均分配 / 本機位移持久化 |
| Tag / SQL92 過濾 | ✅ | 表達式過濾 |
| 多 Topic 訂閱 | ✅ | Topics 屬性，按 Topic 分別 Rebalance |
| 消費重試 + 死信佇列 | ✅ | EnableRetry + MaxReconsumeTimes |
| 順序消費 | ✅ | 佇列鎖定（OrderConsume） |
| Pop 消費 | ✅ | Pop/Ack/BatchAck/ChangeInvisibleTime |
| 消費限流 | ✅ | MaxConcurrentConsume 號誌控制 |

### 事件匯流排

| 功能 | 狀態 | 說明 |
|------|:----:|------|
| Core IEventBus 契約 | ✅ | RocketMqEventBus&lt;TEvent&gt; / RocketMqEventContext |
| 跨行程發佈訂閱 | ✅ | Producer 發佈、Consumer 消費組訂閱，失敗回投重試/死信 |
| 事件中樞工廠 | ✅ | RocketMqEventBusFactory 實作 IEventBusFactory，可掛 EventHub.Factory |
| 名稱正規化 | ✅ | MqEventBusNaming，非法字元→底線、127 字元截斷 |

### 管理與維運

| 功能 | 狀態 | 說明 |
|------|:----:|------|
| Topic/消費組 CRUD | ✅ | 建立/更新/刪除 |
| 訊息查詢 | ✅ | 按 ID / 按 Key |
| 消費統計 / 叢集資訊 | ✅ | GetConsumeStats / GetClusterInfo |
| 位移量管理與重置 | ✅ | 查詢/更新/重置 |

### 協定與相容性

| 服務端版本 | Remoting | gRPC | 說明 |
|-----------|:--------:|:----:|------|
| RocketMQ 4.0 ~ 4.9 | ✅ | — | 完全相容 |
| RocketMQ 5.x（Broker） | ✅ | — | Remoting 向後相容 |
| RocketMQ 5.x（Proxy） | — | ✅ | 透過 GrpcProxyAddress 啟用 |
| 阿里雲 4.x | ✅ | — | AliyunProvider 適配 |
| 華為雲 DMS | ✅ | — | HuaweiProvider 適配 |
| 騰訊雲 TDMQ | ✅ | — | TencentProvider 適配 |

---

## 測試涵蓋範圍

xUnit 測試套件涵蓋核心功能、進階特性、協定相容、雲廠商適配、E2E 整合測試（gRPC 整合案例在未設定 Proxy 時自動跳過）；另含 Apache ACL 2.0 在 Remoting 與 gRPC 兩側的鑑權語意驗證（無憑證/非管理員/錯誤金鑰被拒，管理員可正常收發）。單元測試按測試類別平行執行，`dotnet test XUnitTestRocketMQ/XUnitTest.csproj --filter "Category!=Integration"` 約 4 秒完成；依賴真實 Broker 的整合案例統一進入 RocketMQ 集合串行執行，避免共用主題與消費組互相干擾；排查問題時設定 `ROCKETMQ_TEST_VERBOSE=1` 可恢復框架日誌。CI 以 RID 矩陣執行：單元測試與 AOT 冒煙涵蓋 win-x64 / linux-x64 / linux-arm64 / 龍芯 LA64（QEMU 模擬，必選），win-arm64、osx 雙架構與 linux-musl 雙架構為觀察期，全部支援手動觸發。

另有 NativeAOT 冒煙（`AotSmoke`，涵蓋訊息壓縮、LMQ、ACL 簽名、gRPC 鏈路、死信轉發與 FIFO 順序消費）、NuGet 套件消費 AOT 冒煙（`AotPackageSmoke`，以 PackageReference 消費封裝產物，涵蓋套件資產、源產生序列化、壓縮與端對端收發）、發佈基準收集（`scripts/AotBaseline.cs`，輸出體積/啟動開銷/吞吐並支援閾值判定，CI 寫入管線摘要）、AOT/JIT 效能對照基準（`Benchmark` 工程基於 BenchmarkDotNet，含配置統計；AOT 側由 `AotSmoke --bench` 多輪取樣）、死信維運工具（`scripts/DlqTool.cs`，把 `%DLQ%{group}` 訊息重投到指定主題）與 gRPC 長穩驗收腳本（`scripts/GrpcStability.cs`，CI 夜間執行並注入一次 Proxy 重啟）。

### 本機整合測試（Windows）

```powershell
# 一鍵安裝並啟動 NameServer + Broker + Proxy（每次啟動清空 store）
dotnet run --file scripts/RocketMqSetup.cs

$env:ROCKETMQ_NAMESERVER = "127.0.0.1:9876"
dotnet test XUnitTestRocketMQ\XUnitTest.csproj -c Release

# 停止 / 狀態
dotnet run --file scripts/RocketMqSetup.cs -- --stop
dotnet run --file scripts/RocketMqSetup.cs -- --status
```

本機啟動預設使用低資源模式：三個 JVM 的日誌降到 WARN（不再寫入 `%USERPROFILE%\logs\rocketmqlogs`）、非同步刷盤間隔 5 秒、commitlog 64MB、執行緒集區收緊，並且每次啟動都會清空 store，Broker 始終處於全新狀態。排查問題時用 `--log-level INFO`（或 `$env:ROCKETMQ_TEST_LOG_LEVEL="INFO"`）恢復詳細日誌（日誌量會明顯增大）；不需要 gRPC 時用 `--no-proxy` 少起一個 JVM；需要把 store 指到 RAM 碟時用 `--store-dir R:\RocketMQ\store`。

---

## 參與貢獻

歡迎提交 Issue 與 Pull Request！

1. Fork 本儲存庫
2. 建立特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交變更 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 提交 Pull Request

---

## 授權條款

本專案採用 [MIT License](LICENSE) 開源授權。

---

## 生態版圖

NewLife 生態系按相依分層，各專案獨立版本、獨立發行。下面的版圖在所有 NewLife 儲存庫中逐字一致；每個專案連結到程式碼儲存庫與主 NuGet 套件。

| 專案 | 一句話定位 | 下載量 |
| --- | --- | --- |
| **基礎底座** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | 日誌、設定、快取、網路、序列化、排程與 APM 追蹤。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | 資料中介層：單表百億級、自動分表、讀寫分離，支援 10 多種資料庫。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | 高效能網路庫：單機 177 萬 pps、14.5 Gbps（TCP 1KB，2026-10 實測），400 萬 Tcp 長連線。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | 協定通訊與 RPC 框架，面向 CS 應用程式與物聯網裝置。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | 將 .NET 應用程式安裝為作業系統常駐服務：Windows 服務與 Linux systemd。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | `dotnet new` 專案範本：主控台、Web、API、服務、資料與物聯網。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **資料儲存與訊息** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Redis 用戶端：微秒級延遲、百萬級吞吐，內建訊息佇列。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | 純受控 RocketMQ 用戶端，支援 Apache RocketMQ 與阿里雲訊息佇列。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | MQTT 用戶端與服務端，含阿里雲物聯網接入。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | C# 混合式資料庫：內嵌或網路雙模，關係型、時序、訊息佇列與 KV 於一體。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | 建構於 NewLife 資料堆疊之上的 MySql 驅動與輔助層。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web、AI 與分散式** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | 快速後台開發框架：權限、SSO、OAuth 服務端與 CRUD 樣板。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | 分散式服務平台：節點管理、APM、設定中心、註冊中心與發佈中心。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | 分散式任務與即時資料排程平台，支援分片、重試與回溯補算。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | 統一接入 46 家大模型服務商，附帶開箱即用的 ChatAI Web 對話應用。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **工業與物聯網** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | 物聯網標準庫，定義通訊協定契約與裝置抽象。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | 建構於 NewLife.IoT 之上的 Modbus TCP/RTU/ASCII 實作。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | 建構於 NewLife.IoT 之上的西門子 PLC 協定。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | 建構於 NewLife.IoT 之上的歐姆龍 PLC 協定。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | 建構於 NewLife.IoT 之上的三菱 Melsec PLC 協定。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | 工業資料存取的 OPC 用戶端。 | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | BACnet 樓宇自動化協定堆疊。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | 開源輕量級物聯網平台，含閘道與裝置管理。 | — |
| **車聯網** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | JT/T 808 車輛定位協定：70+ 訊息體，涵蓋 2011/2013/2019 三個版本。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | JT/T 809 部標平台互聯協定，面向運輸管理部門。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | JT/T 1078 車載視訊：RTP 解析、FLV/fMP4 轉封裝與直播推送。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **領域元件** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | 一個套件涵蓋 Excel、Word、PPT、PDF 等 15+ 文件格式。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | 跨平台音訊：編解碼、裝置 IO、DSP、容器與串流處理。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | 視訊編解碼、封裝格式、影格處理與串流。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | 百度／高德／騰訊／天地圖統一介面，支援地理編碼與座標轉換。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | IPv4 歸屬地：離線記憶體映射資料庫 + 線上 API 雙來源。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **工具與產品** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | 開發者工具箱：網路、序列埠、加解密、正規表示式、Modbus 與 MQTT。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | 自架 Git 原始碼管理系統。 | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | 從較舊的宿主啟動時，自動安裝相符的 .NET 執行階段。 | — |

> 徽章取各專案主套件；主題套件、擴充套件與衛星套件見對應 NuGet 頁面。未在表中列出的 30 多個套件（FTP、LoRa、Office 擴充、各類協定驅動等）見 [NewLifeX 組織](https://github.com/NewLifeX) 與 [newlifex.com](https://newlifex.com)。

---

## 新生命開發團隊

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

新生命團隊成立於 2002 年，二十餘年專注 .NET 與物聯網技術生態，主導開源專案近百個（生態版圖收錄 34 個主幹專案），為電力、物流、醫療、工控、網際網路等行業提供軟硬體一體化解決方案；以「學無先後達者為師」為信條。

開源元件在 NuGet 累計下載 <!--nuget-total:start-->6.48M<!--nuget-total:end--> 次（全生態口徑，截至 2026-10）。

網站：https://newlifex.com · 開源組織：https://github.com/NewLifeX

<details>
<summary>微信公眾號：智能大石头（掃碼關注）</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
