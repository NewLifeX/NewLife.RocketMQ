# NewLife.RocketMQ - エンタープライズ向け純マネージド RocketMQ クライアント

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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> この翻訳は[英語版](README.md)より遅れている場合があります。

**エンタープライズ向けの純マネージド RocketMQ クライアント**で、`.NET Framework 4.5+` / `.NET Standard 2.0+` / `.NET Core` / `.NET 5+` をサポートします。  
**C# のみで実装され、外部依存はゼロです（Java、gRPC、Protobuf のサードパーティ製ライブラリは不要）。**

---

## 製品概要

NewLife.RocketMQ は、NewLife チームが .NET エコシステム向けに開発した**エンタープライズ向けの純マネージド RocketMQ クライアント**です。RocketMQ の **Remoting プロトコル（4.x/5.x Broker）** と **gRPC Proxy プロトコル（5.x Proxy）** の両方に対応し、Producer と Consumer の主要機能およびエンタープライズ機能を網羅するとともに、Alibaba Cloud、Huawei Cloud、Tencent Cloud、Apache ACL 認証システム向けの統一アダプターを提供します。

**主な特長**:

| 特長 | 説明 |
|---------|-------------|
| **デュアルプロトコル対応** | Remoting（4.x で成熟・安定）+ gRPC（5.x の将来方向）。自動ルーティング。 |
| **外部依存ゼロ** | Protobuf フィールドコーデックを内蔵（NewLife.Core の SpanWriter/SpanReader 上に手書き実装）。Java や gRPC ランタイムは不要。 |
| **マルチクラウド対応** | 統一インターフェース `ICloudProvider` と、Alibaba Cloud、Huawei Cloud、Tencent Cloud、Apache ACL のアダプターを内蔵。 |
| **本番運用対応** | コンシュームリトライ、デッドレターキュー、トランザクションコールバック確認、順序消費、Pop 消費などのエンタープライズ機能を完備。 |
| **幅広いフレームワーク対応** | .NET Framework 4.5+ から .NET 10 まで。gRPC 機能は .NET Standard 2.1+ で利用可能。 |
| **AOT フレンドリー** | net10.0 ターゲットは NativeAOT/トリミングに対応：フレームのエンコード/デコード、JSON シリアル化、メッセージ圧縮は実行時リフレクションに依存しません。リポジトリには AOT スモークテストと NuGet パッケージ消費側の AOT スモークテストを含み、CI は主要 RID マトリクス（win/linux/osx、musl、Loongson LA64）で AOT 公開と実行を検証します |
| **実行時ローカライズ** | ログ、プロトコル解析例外、公開 API 例外、構成の説明は英語を原文とし、31 言語（132 エントリ）の単語リストを内蔵。`Localization.Culture` に従って自動翻訳し、英語にフォールバックします。接頭辞付きログは接頭辞を組み立てる前に翻訳され、接頭辞は単語リストの検索対象になりません |
| **高性能** | NewLife.Net の高性能ネットワーク層を基盤に、接続再利用、VIP チャネル、メッセージ圧縮、並行制御を実現。 |

---

## インストール

```powershell
# NuGet パッケージマネージャー
Install-Package NewLife.RocketMQ

# .NET CLI
dotnet add package NewLife.RocketMQ
```

```xml
<!-- PackageReference -->
<PackageReference Include="NewLife.RocketMQ" Version="3.0.*" />
```

---

## クイックスタート

### メッセージの送信

```csharp
using NewLife.RocketMQ;

var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    Group = "producer_group"
};
producer.Start();

// 同期送信
var result = producer.Publish("Hello RocketMQ!");
Console.WriteLine($"Message ID: {result.MsgId}");

// 非同期送信
await producer.PublishAsync("async message");

// バッチ送信
await producer.PublishBatch(new[] { "message1", "message2", "message3" });
```

### メッセージの消費

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
    return true; // true を返すと消費成功を意味します
};

consumer.Start();
```

### gRPC（5.x Proxy）アクセス

```csharp
// Producer：GrpcProxyAddress を設定すると 5.x gRPC 経路で通信します（netstandard2.1+ が必要）
var producer = new Producer
{
    Topic = "test_topic",
    Group = "producer_group",
    NameServerAddress = "127.0.0.1:9876",
    GrpcProxyAddress = "http://127.0.0.1:8081",
};
producer.Start();
await producer.PublishAsync("gRPC message");

// Consumer：起動時に購読関係を自動報告（Telemetry 長接続）。受信と ACK は gRPC で行います
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
// ... 業務処理 ...
await consumer.AckMessageViaGrpcAsync("test_topic",
[
    .. messages.Select(m => new AckMessageEntry
    {
        ReceiptHandle = m.SystemProperties?.ReceiptHandle,
        MessageId = m.SystemProperties?.MessageId,
    }),
]);
```

> ロングポーリング消費では `GrpcPushConsumer` を直接利用することもできます（PUSH_CONSUMER セマンティクスで、自動 ACK とリトライを内蔵）。
>
> **5.x POP のリトライとデッドレター**：消費に失敗したメッセージは Broker が `%RETRY%{group}_{topic}` に書き込み、`popFromRetryProbability` に従って元トピックのプル応答にマージし直します。クライアントは元トピックを消費するだけで済みます。配送回数が `GrpcPushConsumer.MaxDeliveryAttempts`（既定 16）に達すると、メッセージは `%DLQ%{group}` に移動します。手動受信のシナリオでは `Consumer.ForwardToDeadLetterQueueViaGrpcAsync` を呼び出します（`GetGrpcDlqTopic()` はデッドレタートピック名を返します）。不可視時間の下限は 10 秒です（ライブラリ内部で自動的に確定）。それより小さい値は Broker に拒否されます。
>
> デッドレターのメッセージは 1 回で処理し切る必要があります：`dotnet run --file scripts/DlqTool.cs -- --group <コンシューマーグループ> --to <元トピック>` はデッドレターキューからメッセージを再公開し、再公開に成功したメッセージだけを ACK します。デッドレターのメッセージをプルすると消費オフセットが進み、ACK していないメッセージは Broker によって `%RETRY%{group}_%DLQ%{group}` へ移動します。そのため、デッドレターキューの読み取り専用の参照はサポートされません。
>
> **FIFO 順序メッセージとトランザクションメッセージ**：トピック種別はメッセージ種別と一致させる必要があり、`CreateTopic(topic, queueNum, 0, TopicMessageType.Fifo/Transaction)` で作成します。FIFO 送信では `Message.MessageGroup` を設定します。Consumer 側では `GrpcPushConsumer.Orderly = true` を設定すると直列に処理され、順序が保持されます。トランザクションメッセージでは、ハーフメッセージの送信に `PublishTransactionViaGrpcAsync`、コミットまたはロールバックに `EndTransactionViaGrpcAsync` を使用します（コミットしない場合、Broker がタイムアウト後にロールバックします）。

### 遅延メッセージ

```csharp
// 18 段階のプリセット遅延レベル
producer.PublishDelay("delayed message", DelayTimeLevels.s30);

// gRPC モードでは任意の遅延に対応（netstandard2.1+ が必要）
producer.GrpcProxyAddress = "http://127.0.0.1:8081";
await producer.PublishDelayViaGrpcAsync("arbitrary delay", DateTime.Now.AddMinutes(30));
```

### トランザクションメッセージ

```csharp
var producer = new Producer
{
    Topic = "tx_topic",
    Group = "tx_group",
    NameServerAddress = "127.0.0.1:9876"
};

// トランザクションのコールバック確認
producer.OnCheckTransaction = (msg, transactionId) =>
{
    var success = CheckLocalTransaction(transactionId);
    return success ? TransactionState.Commit : TransactionState.Rollback;
};
producer.Start();

// ハーフメッセージ送信 -> ローカルトランザクション実行 -> コミット/ロールバック
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

### 順序メッセージ

```csharp
// 同じキーのメッセージは同じキューに送られます
var queue = producer.SelectQueue("order_123");
producer.Publish("ordered message 1", queue);
producer.Publish("ordered message 2", queue);

// Consumer 側で順序消費を有効化
consumer.OrderConsume = true;
```

### リクエスト-リプライパターン

```csharp
// Producer がリクエストを送信（同期/非同期）
var response = producer.Request("request message", timeout: 5000);
var reply = await producer.RequestAsync("async request", timeout: 5000);

// Consumer がリプライを返す
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

### デュアルプロトコルメッセージ変換

```csharp
// Remoting メッセージを gRPC メッセージへ変換（netstandard2.1+ が必要）
var grpcMsg = message.ToGrpcMessage();

// gRPC メッセージを拡張 Remoting メッセージ / 基本メッセージへ変換
var ext = grpcMsg.ToMessageExt();   // MessageExt（すべてのシステムプロパティを含む）
var msg = grpcMsg.ToMessage();      // Message（基本フィールド）
```

---

## Consumer の高度な機能

### コンシュームリトライとデッドレターキュー

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    EnableRetry = true,           // コンシュームリトライを有効化
    MaxReconsumeTimes = 3         // 最大リトライ回数。超過したメッセージは %DLQ% デッドレターキューへ
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        try { ProcessMessage(msg); }
        catch { return false; } // false を返すとリトライが発生します
    }
    return true;
};
```

### タグ / SQL92 フィルタリング

```csharp
// タグフィルタリング
consumer.Tags = "TagA || TagB";

// SQL92 式フィルタリング
consumer.ExpressionType = "SQL92";
consumer.Subscription = "age > 18 AND city = 'Shanghai'";
```

### マルチトピック購読

```csharp
consumer.Topics = "topic1;topic2;topic3";
```

### Pop 消費モード

```csharp
// Pop 消費（手動 ACK）
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
        await consumer.ChangeInvisibleTimeAsync(msg, 30000); // 処理時間を延長
    }
}
```

### 消費スロットリング / VIP チャネル / メッセージ圧縮

```csharp
consumer.MaxConcurrentConsume = 10; // 同時に処理するメッセージは最大 10 件

producer.VipChannelEnabled = true;  // VIP チャネル（BrokerPort - 2）を有効化
producer.CompressOverBytes = 4096;  // 4KB を超えるメッセージボディを自動で ZLIB 圧縮
```

### HTTP ブリッジ（MQ -> WebAPI）

消費したメッセージを WebAPI へ転送する場合や、外部からメッセージを発行できる WebAPI を公開する場合は、既存の API を組み合わせて実現できます。詳しくは [HTTP ブリッジガイド](Doc/HttpBridge_Guide.md) を参照してください。

---

## クラウドプロバイダー接続

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
        InstanceId = "MQ_INST_xxx"  // 省略可能。アドレスから自動解析されます
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

### Apache RocketMQ ACL 認証

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

## NativeAOT 公開

`net10.0` ターゲットのみが NativeAOT/トリミングに対応します（旧ターゲットフレームワークにはトリミングの概念がなく、動作は変わりません）。パッケージにはソース生成のメンバーアクセサーとメタデータルートが既に含まれているため、利用時に追加の構成は不要です：

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

検証済みのシナリオ（ネイティブ実行ファイルで計測）：フレームのエンコード/デコード、リクエストヘッダーのプロパティ辞書、JSON シリアル化、メッセージ圧縮（ZLIB の往復と展開上限）、LMQ プロパティ、ACL 署名、クラウドプロバイダーアダプター、デュアルプロトコル変換、エンドツーエンドの送受信（通常/オブジェクトボディ/バッチ/遅延/大容量メッセージ圧縮、消費、オフセット照会、順序ロック、POP）、および gRPC 5.x 経路（ルーティング、送受信 ACK、プッシュ消費、デッドレター転送、FIFO 順序消費）。

CI カバレッジ：win-x64（ローカル RocketMQ の完全なエンドツーエンドを含む）、linux-x64、linux-arm64、win-arm64、osx-x64/osx-arm64、linux-musl-x64/arm64 はそれぞれネイティブ公開して実行します。Loongson linux-loongarch64 は QEMU ユーザーモードエミュレーションと Loongson SDK で実行します（現在このプラットフォームには公式の NativeAOT サポートがないため、AOT 相当のエンジニアリング JIT スモークテストを判定基準としています）。

**カスタム型のオブジェクトボディは登録が必要**：カスタム型（POCO）をメッセージボディとして使用すると、トリマーがその公開プロパティを既定で削除し、オブジェクトボディは空のオブジェクト `{}` として暗黙にシリアル化されます。アプリケーションの起動時に、メッセージボディとして使用する型を 1 回登録してください：

```csharp
MqAot.RegisterBodyType<MyOrder>();
MqAot.RegisterBodyType<MyEvent>();
```

`MqAot.RegisterBodyType<T>()` は net10.0 でのみトリミング保持のセマンティクスを持ち、他のターゲットでは何もしません。そのため、マルチターゲットのプロジェクトでも同じ起動コードを共有できます。ライブラリと同じパターン（`[DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MyOrder))]` + `[ModuleInitializer]`）で独自のメタデータルートを宣言することもできます。

---

## アーキテクチャ概要

```
MqBase（業務基底クラス）
├── Producer
└── Consumer

通信層
├── Remoting プロトコル（4.x/5.x Broker）
│   ├── ClusterClient（TCP 長接続、Opaque 再利用）
│   ├── NameClient（ルート探索、30 秒ポーリング）
│   └── BrokerClient（ハートビート/登録解除）
│
└── gRPC プロトコル（5.x Proxy、netstandard2.1+）
    ├── GrpcClient（HTTP/2、Unary + Server Streaming + 書き込み専用長接続）
    ├── GrpcMessagingService（11 個の RPC メソッド）
    ├── GrpcTelemetrySession（リソース報告長接続：定期再送 + 切断時再接続）
    ├── MessageConverter（Remoting ↔ gRPC メッセージ変換）
    └── フィールドコーデック（SpanWriter/SpanReader による手書き Protobuf）

クラウドプロバイダーアダプター層
├── AliyunProvider  （Alibaba Cloud：インスタンス ID ルーティング + HTTP NameServer）
├── HuaweiProvider  （Huawei Cloud：SSL/TLS + インスタンス ID ルーティング）
├── TencentProvider （Tencent Cloud：Namespace プレフィックスルーティング）
└── AclProvider     （Apache ACL：HMAC-SHA1 署名）
```

詳しくは [要件ドキュメント](Doc/需求文档.md)、[機能一覧](Doc/功能清单.md)、[アーキテクチャ設計](Doc/架构设计.md)、[競合分析](Doc/竞品分析.md)、[HTTP ブリッジガイド](Doc/HttpBridge_Guide.md)、[多言語ローカライズ](Doc/多语言本地化.md) を参照してください。

---

## 機能一覧

### Producer

| 機能 | 状態 | 説明 |
|---------|:------:|-------------|
| 同期/非同期/一方向送信 | ✅ | Publish / PublishAsync / PublishOneway |
| バッチメッセージ送信 | ✅ | PublishBatch。複数メッセージを 1 リクエストにまとめて送信 |
| 遅延メッセージ | ✅ | 18 段階のプリセット + gRPC による任意遅延 |
| トランザクションメッセージ | ✅ | ハーフメッセージ + コミット/ロールバック + コールバック確認 |
| 順序メッセージ | ✅ | 指定した MessageQueue へ送信 |
| リクエスト-リプライ | ✅ | 同期/非同期のリクエスト-リプライ |
| メッセージ圧縮 | ✅ | CompressOverBytes のしきい値を超えると自動で ZLIB 圧縮 |
| メッセージトレース | ✅ | AsyncTraceDispatcher + MessageTraceHook |

### Consumer

| 機能 | 状態 | 説明 |
|---------|:------:|-------------|
| プル消費 / 消費スケジューリング | ✅ | ロングポーリングプルと自動キュー割り当て |
| クラスタ/ブロードキャスト消費 | ✅ | Rebalance の平均割り当て / ローカルオフセット永続化 |
| タグ / SQL92 フィルタリング | ✅ | 式によるフィルタリング |
| マルチトピック購読 | ✅ | Topics プロパティ。トピックごとに Rebalance |
| コンシュームリトライ + デッドレターキュー | ✅ | EnableRetry + MaxReconsumeTimes |
| 順序消費 | ✅ | キューロック（OrderConsume） |
| Pop 消費 | ✅ | Pop/Ack/BatchAck/ChangeInvisibleTime |
| 消費スロットリング | ✅ | MaxConcurrentConsume によるセマフォ制御 |

### 管理と運用

| 機能 | 状態 | 説明 |
|---------|:------:|-------------|
| トピック/コンシューマーグループの CRUD | ✅ | 作成/更新/削除 |
| メッセージ照会 | ✅ | ID 指定 / Key 指定 |
| 消費統計 / クラスタ情報 | ✅ | GetConsumeStats / GetClusterInfo |
| オフセット管理とリセット | ✅ | 照会/更新/リセット |

### プロトコルと互換性

| サーバーバージョン | Remoting | gRPC | 説明 |
|----------------|:--------:|:----:|-------------|
| RocketMQ 4.0 ~ 4.9 | ✅ | — | 完全互換 |
| RocketMQ 5.x（Broker） | ✅ | — | Remoting は後方互換 |
| RocketMQ 5.x（Proxy） | — | ✅ | GrpcProxyAddress で有効化 |
| Alibaba Cloud 4.x | ✅ | — | AliyunProvider アダプター |
| Huawei Cloud DMS | ✅ | — | HuaweiProvider アダプター |
| Tencent Cloud TDMQ | ✅ | — | TencentProvider アダプター |

---

## テストカバレッジ

xUnit テストスイートは、コア機能、高度な機能、プロトコル互換性、クラウドプロバイダーアダプター、E2E 統合テストを網羅しています（gRPC 統合ケースは Proxy が未構成の場合に自動的にスキップされます）。また、Apache ACL 2.0 の認証セマンティクスを Remoting と gRPC の両方で検証します（資格情報なし、非管理者ユーザー、誤ったキーによるリクエストは拒否され、管理者は正常に送受信できます）。単体テストはテストクラス単位で並列実行され、`dotnet test XUnitTestRocketMQ/XUnitTest.csproj --filter "Category!=Integration"` は約 4 秒で完了します。実際の Broker に依存する統合ケースは RocketMQ コレクションに配置され、共有トピックやコンシューマーグループによる干渉を避けるため直列実行されます。トラブルシューティング時にフレームワークのログを復元するには `ROCKETMQ_TEST_VERBOSE=1` を設定します。CI は RID マトリクスで実行されます。単体テストと AOT スモークテストは win-x64 / linux-x64 / linux-arm64 / Loongson LA64（QEMU エミュレーション、必須）をカバーし、win-arm64、osx の 2 アーキテクチャ、linux-musl の 2 アーキテクチャは観察期間中です。すべて手動トリガーに対応しています。

あわせて以下も含まれます：NativeAOT スモークテスト（`AotSmoke`。メッセージ圧縮、LMQ、ACL 署名、gRPC 経路、デッドレター転送、FIFO 順序消費を含む）、NuGet パッケージ消費側の AOT スモークテスト（`AotPackageSmoke`。PackageReference でパッケージ化した成果物を消費し、パッケージアセット、ソース生成シリアル化、圧縮、エンドツーエンドの送受信をカバー）、公開ベースライン収集（`scripts/AotBaseline.cs`。サイズ/起動オーバーヘッド/スループットを報告し、しきい値チェックと CI のパイプラインサマリーへの書き出しを行う）、AOT/JIT 性能比較ベンチマーク（BenchmarkDotNet ベースの `Benchmark` プロジェクト。割り当て統計を含む。AOT 側は `AotSmoke --bench` による多回サンプリングを使用）、デッドレター運用ツール（`scripts/DlqTool.cs`。`%DLQ%{group}` のメッセージを指定トピックへ再公開）、gRPC 安定性受け入れスクリプト（`scripts/GrpcStability.cs`。CI で毎晩実行し、Proxy の再起動を 1 回注入）。

### ローカル統合テスト（Windows）

```powershell
# NameServer + Broker + Proxy のワンステップでのインストールと起動（ストアは毎回クリアされます）
dotnet run --file scripts/RocketMqSetup.cs

$env:ROCKETMQ_NAMESERVER = "127.0.0.1:9876"
dotnet test XUnitTestRocketMQ\XUnitTest.csproj -c Release

# 停止 / 状態確認
dotnet run --file scripts/RocketMqSetup.cs -- --stop
dotnet run --file scripts/RocketMqSetup.cs -- --status
```

ローカル起動は既定で低リソースモードを使用します。3 つの JVM のログは WARN に抑えられ（`%USERPROFILE%\logs\rocketmqlogs` へは出力されません）、非同期フラッシュ間隔は 5 秒、commitlog は 64MB、スレッドプールは絞り込まれ、ストアは毎回クリアされるため Broker は常に新しい状態です。トラブルシューティング時は `--log-level INFO`（または `$env:ROCKETMQ_TEST_LOG_LEVEL="INFO"`）で詳細ログを復元できます（ログ量は明らかに増えます）。gRPC が不要な場合は `--no-proxy` で JVM を 1 つ減らせます。ストアを RAM ディスクに置く場合は `--store-dir R:\RocketMQ\store` を使用します。

---

## コントリビューション

Issue と Pull Request を歓迎します！

1. このリポジトリをフォークします
2. 機能ブランチを作成します（`git checkout -b feature/AmazingFeature`）
3. 変更をコミットします（`git commit -m 'Add some AmazingFeature'`）
4. ブランチにプッシュします（`git push origin feature/AmazingFeature`）
5. Pull Request を作成します

---

## ライセンス

本プロジェクトは [MIT License](LICENSE) に基づいて公開されています。

---

## エコシステムマップ

NewLife は、独立したバージョン管理を行うオープンソースプロジェクトの階層型エコシステムです。以下のマップはすべての NewLife リポジトリで逐語的に同一の内容を共有しており、各プロジェクトはコードリポジトリと主要な NuGet パッケージにリンクしています。

| プロジェクト | 概要 | ダウンロード数 |
| --- | --- | --- |
| **基盤** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | ログ、構成、キャッシュ、ネットワーク、シリアライズ、スケジューリング、APM トレーシング。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | データミドルウェア：単一テーブルで数百億件級、自動シャーディング、読み書き分離、10 種類以上のデータベースに対応。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | 高性能ネットワークライブラリ：単一マシンで 177 万 pps、14.5 Gbps（TCP 1KB、2026-10 実測）、400 万 Tcp 長接続。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | CS アプリケーションと IoT デバイス向けのプロトコル通信と RPC フレームワーク。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | .NET アプリケーションを OS デーモンとしてインストール：Windows サービスと Linux systemd。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | コンソール、Web、API、サービス、データ、IoT 向けの `dotnet new` プロジェクトテンプレート。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **データとメッセージング** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | マイクロ秒級の遅延、数百万 ops/s のスループット、メッセージキュー内蔵の Redis クライアント。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Apache RocketMQ と Alibaba Cloud Message Queue に対応した純マネージド RocketMQ クライアント。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | MQTT クライアントとサーバー。Alibaba Cloud IoT への接続を含みます。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | C# 製ハイブリッドデータベース：組み込み/ネットワークの両モード、リレーショナル、時系列、メッセージキュー、KV を 1 つに。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | NewLife データスタック上に構築された MySql ドライバーとヘルパー層。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web、AI、分散** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | 高速管理プラットフォームフレームワーク：権限、SSO、OAuth サーバー、CRUD スキャフォールディング。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | 分散サービスプラットフォーム：ノード管理、APM、構成センター、登録センター、リリースセンター。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | シャーディング、リトライ、バックフィルに対応した分散タスクとリアルタイムデータのスケジューリングプラットフォーム。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | 46 社の LLM プロバイダーに統一接続するクライアントと、すぐに使える Web チャットアプリ ChatAI。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **産業と IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | プロトコル契約とデバイス抽象を定義する IoT 標準ライブラリ。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | NewLife.IoT 上に実装された Modbus TCP/RTU/ASCII。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | NewLife.IoT 上に実装されたシーメンス PLC プロトコル。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | NewLife.IoT 上に実装されたオムロン PLC プロトコル。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | NewLife.IoT 上に実装された三菱 Melsec PLC プロトコル。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | 産業データアクセス向けの OPC クライアント。 | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | BACnet ビル管理プロトコルスタック。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | ゲートウェイとデバイス管理を備えた軽量なオープンソース IoT プラットフォーム。 | — |
| **車載テレマティクス** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | JT/T 808 車両位置情報プロトコル：70 以上のメッセージボディ、2011/2013/2019 に対応。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | 運輸管理部門向けの JT/T 809 プラットフォーム間プロトコル。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | JT/T 1078 車載ビデオ：RTP 解析、FLV/fMP4 リマックス、ライブ配信。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **ドメインコンポーネント** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Excel、Word、PPT、PDF など 15 以上のドキュメント形式を 1 パッケージで。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | クロスプラットフォーム音声：コーデック、デバイス IO、DSP、コンテナ、ストリーミング。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | ビデオコーデック、コンテナ形式、フレーム処理、ストリーミング。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | 百度地図、高徳地図、テンセント地図、天地図を統一インターフェイス化。ジオコーディングと座標変換に対応。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | IPv4 位置情報：オフラインのメモリマップデータベースとオンライン API のフォールバック。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **ツールと製品** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | 開発者向けツールボックス：ネットワーク、シリアルポート、暗号化、正規表現、Modbus、MQTT。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | セルフホスト型 Git ソースコード管理システム。 | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | 古いホストから起動した際に対応する .NET ランタイムを自動インストールします。 | — |

> バッジは各プロジェクトの主要パッケージを示します。テーマ、拡張、サテライトパッケージは各 NuGet ページを参照してください。ここに記載していない 30 以上のパッケージ（FTP、LoRa、Office 拡張、各種プロトコルドライバーなど）は [NewLifeX 組織](https://github.com/NewLifeX) と [newlifex.com](https://newlifex.com) で公開しています。

---

## 新生命開発チーム

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

新生命チームは 2002 年に設立され、20 年以上にわたり .NET と IoT の技術エコシステムに注力し、100 近いオープンソースプロジェクトを主導し（うち 34 が主要プロジェクト）、電力、物流、医療、産業制御、インターネットなどの業界にソフトウェアとハードウェアを一体化したソリューションを提供しています。「学問に先後なし、達する者が師である」を信条としています。

オープンソースコンポーネントは NuGet で累計 <!--nuget-total:start-->6.48M<!--nuget-total:end--> 回ダウンロードされています（エコシステム全体、2026-10 時点）。

ウェブサイト：https://newlifex.com · オープンソース組織：https://github.com/NewLifeX

<details>
<summary>WeChat 公式アカウント：智能大石头（スキャンしてフォロー）</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
