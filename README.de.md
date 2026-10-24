# NewLife.RocketMQ - Vollständig verwalteter RocketMQ-Client in Enterprise-Qualität

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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> Diese Übersetzung kann hinter der [englischen Version](README.md) zurückliegen.

**Ein RocketMQ-Client in Enterprise-Qualität, vollständig verwaltet**, unterstützt `.NET Framework 4.5+` / `.NET Standard 2.0+` / `.NET Core` / `.NET 5+`.  
**Vollständig in C# implementiert, ohne externe Abhängigkeiten (keine Java-, gRPC- oder Protobuf-Bibliotheken von Drittanbietern erforderlich).**

---

## Produktübersicht

NewLife.RocketMQ ist ein **vollständig verwalteter RocketMQ-Client in Enterprise-Qualität**, entwickelt vom NewLife-Team für das .NET-Ökosystem. Er unterstützt sowohl das RocketMQ-**Remoting-Protokoll (4.x/5.x Broker)** als auch das **gRPC-Proxy-Protokoll (5.x Proxy)** und deckt alle Kernfunktionen von Producer und Consumer sowie Enterprise-Funktionen ab, mit einheitlichen Adaptern für Alibaba Cloud, Huawei Cloud, Tencent Cloud und das Apache-ACL-Authentifizierungssystem.

**Zentrale Vorteile**:

| Funktion | Beschreibung |
|---------|-------------|
| **Dual-Protokoll-Unterstützung** | Remoting (ausgereift und stabil auf 4.x) + gRPC (zukunftssicher auf 5.x), mit automatischem Routing |
| **Keine externen Abhängigkeiten** | Eingebauter Protobuf-Feld-Codec (handgeschrieben auf SpanWriter/SpanReader von NewLife.Core), keine Java- oder gRPC-Laufzeit erforderlich |
| **Multi-Cloud-Unterstützung** | Einheitliche `ICloudProvider`-Schnittstelle mit eingebauten Adaptern für Alibaba Cloud, Huawei Cloud, Tencent Cloud und Apache ACL |
| **Produktionsreif** | Vollständige Enterprise-Funktionen: Consume-Retry, Dead-Letter-Queue, Transaktions-Checkback, geordnete Verarbeitung, Pop-Consumption und mehr |
| **Event-Bus** | Eingebauter Event-Bus auf Basis des Core-Vertrags `IEventBus`: Ereignisse werden über Producer/Consumer prozessübergreifend veröffentlicht und abonniert, mit automatischem Retry und Dead-Letter-Behandlung |
| **Breiteste Framework-Abdeckung** | Von .NET Framework 4.5+ bis .NET 10; gRPC-Funktionen sind ab .NET Standard 2.1+ verfügbar |
| **AOT-freundlich** | Das Ziel net10.0 unterstützt NativeAOT/Trimming: Frame-Codierung/-Decodierung, JSON-Serialisierung und Nachrichtenkomprimierung benötigen keine Laufzeitreflexion. Das Repository enthält einen AOT-Smoke-Test sowie einen AOT-Smoke-Test für die Nutzung des NuGet-Pakets; CI validiert AOT-Publishing und -Ausführung auf einer RID-Matrix gängiger Plattformen (win/linux/osx, musl und Loongson LA64) |
| **Laufzeit-Lokalisierung** | Logs, Ausnahmen der Protokollanalyse, Ausnahmen öffentlicher APIs und Konfigurationsbeschreibungen verwenden Englisch als Quelltext, mit eingebauten Wortlisten für 31 Sprachen, automatisch übersetzt gemäß `Localization.Culture` mit englischem Fallback; Logs mit Präfix werden vor dem Anhängen des Präfixes übersetzt, und das Präfix nimmt nicht an der Wortlistensuche teil |
| **Hohe Leistung** | Aufgebaut auf der Hochleistungs-Netzwerkschicht von NewLife.Net: Verbindungswiederverwendung, VIP-Kanal, Nachrichtenkomprimierung und Nebenläufigkeitskontrolle |

---

## Installation

```powershell
# NuGet-Paketmanager
Install-Package NewLife.RocketMQ

# .NET CLI
dotnet add package NewLife.RocketMQ
```

```xml
<!-- PackageReference -->
<PackageReference Include="NewLife.RocketMQ" Version="4.0.*" />
```

---

## Schnellstart

### Nachrichten senden

```csharp
using NewLife.RocketMQ;

var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    Group = "producer_group"
};
producer.Start();

// Synchrones Senden
var result = producer.Publish("Hello RocketMQ!");
Console.WriteLine($"Message ID: {result.MsgId}");

// Asynchrones Senden
await producer.PublishAsync("async message");

// Batch-Versand
await producer.PublishBatch(new[] { "message1", "message2", "message3" });
```

### Nachrichten empfangen

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
    return true; // true signalisiert eine erfolgreiche Verarbeitung
};

consumer.Start();
```

### gRPC-Zugang (5.x Proxy)

```csharp
// Producer: Mit GrpcProxyAddress wird der Datenverkehr über den gRPC-Pfad von 5.x geleitet (erfordert netstandard2.1+)
var producer = new Producer
{
    Topic = "test_topic",
    Group = "producer_group",
    NameServerAddress = "127.0.0.1:9876",
    GrpcProxyAddress = "http://127.0.0.1:8081",
};
producer.Start();
await producer.PublishAsync("gRPC message");

// Consumer: Abonnementbeziehungen werden beim Start automatisch gemeldet (Telemetry-Langverbindung); Empfang und Bestätigung laufen über gRPC
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
// ... Geschäftsverarbeitung ...
await consumer.AckMessageViaGrpcAsync("test_topic",
[
    .. messages.Select(m => new AckMessageEntry
    {
        ReceiptHandle = m.SystemProperties?.ReceiptHandle,
        MessageId = m.SystemProperties?.MessageId,
    }),
]);
```

> Lang-Polling-Consumption kann auch direkt `GrpcPushConsumer` verwenden (PUSH_CONSUMER-Semantik mit eingebautem automatischem Ack und Retry).
>
> **5.x POP-Retry und Dead Letters**: Nachrichten, deren Verarbeitung fehlschlägt, werden vom Broker nach `%RETRY%{group}_{topic}` geschrieben und gemäß `popFromRetryProbability` wieder in die Pull-Antwort des ursprünglichen Topics eingefügt; der Client muss nur das ursprüngliche Topic konsumieren. Wenn die Zustellungsanzahl `GrpcPushConsumer.MaxDeliveryAttempts` erreicht (Standard 16), wird die Nachricht nach `%DLQ%{group}` verschoben; im Szenario des manuellen Empfangs rufen Sie `Consumer.ForwardToDeadLetterQueueViaGrpcAsync` auf (`GetGrpcDlqTopic()` liefert den Namen des Dead-Letter-Topics). Die Untergrenze der Unsichtbarkeitszeit beträgt 10 Sekunden (wird innerhalb der Bibliothek automatisch angeglichen); kleinere Werte lehnt der Broker ab.
>
> Dead-Letter-Nachrichten müssen in einem Durchgang verarbeitet werden: `dotnet run --file scripts/DlqTool.cs -- --group <consumer group> --to <original topic>` veröffentlicht Nachrichten aus der Dead-Letter-Queue erneut im angegebenen Topic und bestätigt nur die erfolgreich erneut veröffentlichten Nachrichten. Das Abrufen von Dead-Letter-Nachrichten erhöht den Consume-Offset, und nicht bestätigte Nachrichten werden vom Broker nach `%RETRY%{group}_%DLQ%{group}` verschoben; daher wird das rein lesende Durchsuchen der Dead-Letter-Queue nicht unterstützt.
>
> **FIFO-geordnete Nachrichten und transaktionale Nachrichten**: Der Topic-Typ muss zum Nachrichtentyp passen; erstellen Sie ihn mit `CreateTopic(topic, queueNum, 0, TopicMessageType.Fifo/Transaction)`. Für FIFO-Versand setzen Sie `Message.MessageGroup`; auf der Consumer-Seite setzen Sie `GrpcPushConsumer.Orderly = true`, um seriell und reihenfolgetreu zu verarbeiten. Für transaktionale Nachrichten verwenden Sie `PublishTransactionViaGrpcAsync` zum Senden der halben Nachricht und `EndTransactionViaGrpcAsync` zum Commit oder Rollback (ohne Commit führt der Broker nach einem Timeout einen Rollback durch).

### Verzögerte Nachrichten

```csharp
// 18 voreingestellte Verzögerungsstufen
producer.PublishDelay("delayed message", DelayTimeLevels.s30);

// Der gRPC-Modus unterstützt beliebige Verzögerungen (erfordert netstandard2.1+)
producer.GrpcProxyAddress = "http://127.0.0.1:8081";
await producer.PublishDelayViaGrpcAsync("arbitrary delay", DateTime.Now.AddMinutes(30));
```

### Transaktionale Nachrichten

```csharp
var producer = new Producer
{
    Topic = "tx_topic",
    Group = "tx_group",
    NameServerAddress = "127.0.0.1:9876"
};

// Callback für die Transaktionsprüfung
producer.OnCheckTransaction = (msg, transactionId) =>
{
    var success = CheckLocalTransaction(transactionId);
    return success ? TransactionState.Commit : TransactionState.Rollback;
};
producer.Start();

// Halbe Nachricht senden -> lokale Transaktion ausführen -> Commit/Rollback
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

### Geordnete Nachrichten

```csharp
// Nachrichten mit demselben Schlüssel gelangen in dieselbe Warteschlange
var queue = producer.SelectQueue("order_123");
producer.Publish("ordered message 1", queue);
producer.Publish("ordered message 2", queue);

// Auf der Consumer-Seite die geordnete Verarbeitung aktivieren
consumer.OrderConsume = true;
```

### Request-Reply-Muster

```csharp
// Producer sendet eine Anfrage (synchron/asynchron)
var response = producer.Request("request message", timeout: 5000);
var reply = await producer.RequestAsync("async request", timeout: 5000);

// Consumer antwortet
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

### Dual-Protokoll-Nachrichtenkonvertierung

```csharp
// Eine Remoting-Nachricht in eine gRPC-Nachricht konvertieren (erfordert netstandard2.1+)
var grpcMsg = message.ToGrpcMessage();

// Eine gRPC-Nachricht in eine erweiterte Remoting-Nachricht / Basisnachricht konvertieren
var ext = grpcMsg.ToMessageExt();   // MessageExt (enthält alle Systemeigenschaften)
var msg = grpcMsg.ToMessage();      // Message (Basisfelder)
```

---

### Event-Bus

```csharp
using NewLife.Messaging;
using NewLife.RocketMQ;

// Ereignis-Topics dürfen Punkte enthalten; Namen werden auf gültige RocketMQ-Namen normalisiert (order.created -> order_created)
using var bus = new RocketMqEventBus<OrderPaidEvent>("127.0.0.1:9876", "order.created", "order_service");

// Abonnieren: Wirft der Handler eine Ausnahme, wird die Nachricht erneut zugestellt und landet schließlich in der Dead-Letter-Queue
await bus.SubscribeAsync((OrderPaidEvent e, IEventContext ctx) =>
{
    Console.WriteLine($"Bestellung bezahlt: {e.OrderId}");
    return Task.CompletedTask;
}, "billing");

// Veröffentlichen: Zustellung über den Broker; lokale Abonnenten erhalten sie ebenfalls über ihre Consumergruppe
await bus.PublishAsync(new OrderPaidEvent { OrderId = 1001 });

// An einen Event-Hub anbinden: Die Factory erstellt und cached einen Bus pro Topic
var hub = new EventHub<OrderPaidEvent>
{
    Factory = new RocketMqEventBusFactory("127.0.0.1:9876"),
};
```

> Die Consumergruppe verwendet standardmäßig die Identität des aktuellen Knotens, sodass jeder Knoten jedes Ereignis erhält. Übergib dieselbe clientId, um auf Competing-Consumer-Semantik umzuschalten. Halte Handler idempotent, da ein teilweiser Batch-Fehler den gesamten Batch erneut zustellt.

---

## Erweiterte Consumer-Funktionen

### Consume-Retry und Dead-Letter-Queue

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    EnableRetry = true,           // Consume-Retry aktivieren
    MaxReconsumeTimes = 3         // Maximale Wiederholungsanzahl; darüber hinausgehende Nachrichten landen in der Dead-Letter-Queue %DLQ%
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        try { ProcessMessage(msg); }
        catch { return false; } // false zurückgeben löst einen erneuten Versuch aus
    }
    return true;
};
```

### Tag-/SQL92-Filterung

```csharp
// Tag-Filterung
consumer.Tags = "TagA || TagB";

// SQL92-Ausdrucksfilterung
consumer.ExpressionType = "SQL92";
consumer.Subscription = "age > 18 AND city = 'Shanghai'";
```

### Multi-Topic-Abonnement

```csharp
consumer.Topics = "topic1;topic2;topic3";
```

### Pop-Consumption-Modus

```csharp
// Pop-Consumption (manuelle Bestätigung)
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
        await consumer.ChangeInvisibleTimeAsync(msg, 30000); // Verarbeitungszeit verlängern
    }
}
```

### Consume-Drosselung / VIP-Kanal / Nachrichtenkomprimierung

```csharp
consumer.MaxConcurrentConsume = 10; // Höchstens 10 Nachrichten gleichzeitig verarbeiten

producer.VipChannelEnabled = true;  // VIP-Kanal aktivieren (BrokerPort - 2)
producer.CompressOverBytes = 4096;  // Nachrichtenkörper über 4 KB automatisch mit ZLIB komprimieren
```

### HTTP-Bridge (MQ -> WebAPI)

Um konsumierte Nachrichten an eine WebAPI weiterzuleiten oder eine WebAPI für die externe Veröffentlichung von Nachrichten bereitzustellen, können Sie die vorhandenen APIs kombinieren; siehe [HTTP-Bridge-Anleitung](Doc/HttpBridge_Guide.md).

---

## Cloud-Provider-Zugang

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
        InstanceId = "MQ_INST_xxx"  // Optional; wird automatisch aus der Adresse ermittelt
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

### Apache RocketMQ ACL-Authentifizierung

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

## NativeAOT-Publishing

Nur das Ziel `net10.0` unterstützt NativeAOT/Trimming (ältere Zielframeworks kennen kein Trimming und verhalten sich unverändert). Das Paket enthält bereits quellgenerierte Member-Accessoren und Metadaten-Roots, daher ist bei der Nutzung keine zusätzliche Konfiguration erforderlich:

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

Überprüfte Szenarien (mit nativen ausführbaren Dateien gemessen): Frame-Codierung/-Decodierung, Eigenschaftswörterbücher der Anfrage-Header, JSON-Serialisierung, Nachrichtenkomprimierung (ZLIB-Roundtrip und Dekomprimierungsgrenzen), LMQ-Eigenschaften, ACL-Signierung, Cloud-Provider-Adapter, Dual-Protokoll-Konvertierung sowie End-to-End-Senden/-Empfangen (normal/Objektkörper/Batch/verzögert/Komprimierung großer Nachrichten, Konsum, Offset-Abfrage, geordnete Sperre, POP) sowie der gRPC-5.x-Pfad (Routing, Sende-/Empfangsbestätigung, Push-Consumption, Dead-Letter-Weiterleitung, FIFO-geordnete Verarbeitung).

CI-Abdeckung: win-x64 (einschließlich vollständigem lokalem RocketMQ-End-to-End), linux-x64, linux-arm64, win-arm64, osx-x64/osx-arm64, linux-musl-x64/arm64 werden jeweils nativ veröffentlicht und ausgeführt; Loongson linux-loongarch64 läuft über QEMU-Benutzermodus-Emulation plus Loongson-SDK (für diese Plattform gibt es derzeit keine offizielle NativeAOT-Unterstützung, daher dient ein AOT-artiger JIT-Smoke-Test als Kriterium).

**Benutzerdefinierte Objektkörper müssen registriert werden**: Wenn ein benutzerdefinierter Typ (POCO) als Nachrichtenkörper verwendet wird, entfernt der Trimmer standardmäßig seine öffentlichen Eigenschaften, und der Objektkörper wird stillschweigend als leeres Objekt `{}` serialisiert. Registrieren Sie jeden als Nachrichtenkörper verwendeten Typ einmal beim Anwendungsstart:

```csharp
MqAot.RegisterBodyType<MyOrder>();
MqAot.RegisterBodyType<MyEvent>();
```

`MqAot.RegisterBodyType<T>()` hat nur auf net10.0 Trimming-Erhaltungssemantik; auf anderen Zielen ist es ein No-Op, sodass Multi-Target-Projekte denselben Startcode verwenden können. Sie können auch eigene Metadaten-Roots nach demselben Muster wie die Bibliothek deklarieren (`[DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MyOrder))]` + `[ModuleInitializer]`).

---

## Architekturübersicht

```
MqBase (Geschäftsbasisklasse)
├── Producer
└── Consumer

Kommunikationsschicht
├── Remoting-Protokoll (4.x/5.x Broker)
│   ├── ClusterClient (TCP-Langverbindung, Opaque-Wiederverwendung)
│   ├── NameClient (Routenermittlung, 30-s-Polling)
│   └── BrokerClient (Heartbeat/Unregister)
│
└── gRPC-Protokoll (5.x Proxy, netstandard2.1+)
    ├── GrpcClient (HTTP/2, Unary + Server Streaming + schreibgeschützte Langverbindung)
    ├── GrpcMessagingService (11 RPC-Methoden)
    ├── GrpcTelemetrySession (Langverbindung zur Ressourcenmeldung: periodisches erneutes Senden + Wiederverbindung bei Abbruch)
    ├── MessageConverter (Konvertierung zwischen Remoting- und gRPC-Nachrichten)
    └── Feld-Codec (handgeschriebenes Protobuf auf Basis von SpanWriter/SpanReader)

Cloud-Provider-Adapterschicht
├── AliyunProvider  (Alibaba Cloud: Instanz-ID-Routing + HTTP-NameServer)
├── HuaweiProvider  (Huawei Cloud: SSL/TLS + Instanz-ID-Routing)
├── TencentProvider (Tencent Cloud: Namespace-Präfix-Routing)
└── AclProvider     (Apache ACL: HMAC-SHA1-Signierung)
```

Siehe [Anforderungsdokument](Doc/需求文档.md), [Funktionsliste](Doc/功能清单.md), [Architekturdesign](Doc/架构设计.md), [Wettbewerbsanalyse](Doc/竞品分析.md), [HTTP-Bridge-Anleitung](Doc/HttpBridge_Guide.md) und [Lokalisierung](Doc/多语言本地化.md).

---

## Funktionsübersicht

### Producer

| Funktion | Status | Beschreibung |
|---------|:------:|-------------|
| Synchroner/asynchroner/One-Way-Versand | ✅ | Publish / PublishAsync / PublishOneway |
| Batch-Nachrichtenversand | ✅ | PublishBatch, fasst mehrere Nachrichten zu einer Anfrage zusammen |
| Verzögerte Nachrichten | ✅ | 18 voreingestellte Stufen + beliebige Verzögerungen über gRPC |
| Transaktionale Nachrichten | ✅ | Halbe Nachricht + Commit/Rollback + Checkback-Callback |
| Geordnete Nachrichten | ✅ | Versand an eine angegebene MessageQueue |
| Request-Reply | ✅ | Synchrone/asynchrone Anfrage-Antwort |
| Nachrichtenkomprimierung | ✅ | Automatisches ZLIB oberhalb der Schwelle CompressOverBytes |
| Message Trace | ✅ | AsyncTraceDispatcher + MessageTraceHook |

### Consumer

| Funktion | Status | Beschreibung |
|---------|:------:|-------------|
| Pull-Consumption / Consume-Planung | ✅ | Long-Polling-Pull mit automatischer Warteschlangenzuweisung |
| Clustered-/Broadcast-Consumption | ✅ | Rebalance mit gleichmäßiger Zuweisung / lokale Offset-Persistenz |
| Tag-/SQL92-Filterung | ✅ | Filterung per Ausdruck |
| Multi-Topic-Abonnement | ✅ | Eigenschaft Topics; Rebalance je Topic |
| Consume-Retry + Dead-Letter-Queue | ✅ | EnableRetry + MaxReconsumeTimes |
| Geordnete Verarbeitung | ✅ | Warteschlangensperre (OrderConsume) |
| Pop-Consumption | ✅ | Pop/Ack/BatchAck/ChangeInvisibleTime |
| Consume-Drosselung | ✅ | Semaphorsteuerung über MaxConcurrentConsume |

### Event-Bus

| Funktion | Status | Beschreibung |
|---------|:------:|-------------|
| Core-IEventBus-Vertrag | ✅ | RocketMqEventBus&lt;TEvent&gt; / RocketMqEventContext |
| Prozessübergreifendes Pub/Sub | ✅ | Producer veröffentlicht, die Consumergruppe des Consumer abonniert; Fehler gehen in Retry/Dead-Letter |
| Event-Hub-Factory | ✅ | RocketMqEventBusFactory implementiert IEventBusFactory und lässt sich an EventHub.Factory anbinden |
| Namensnormalisierung | ✅ | MqEventBusNaming: ungültige Zeichen zu Unterstrichen, Kürzung auf 127 Zeichen |

### Verwaltung und Betrieb

| Funktion | Status | Beschreibung |
|---------|:------:|-------------|
| Topic-/Consumer-Group-CRUD | ✅ | Erstellen/Aktualisieren/Löschen |
| Nachrichtenabfrage | ✅ | Nach ID / nach Key |
| Consume-Statistik / Clusterinformationen | ✅ | GetConsumeStats / GetClusterInfo |
| Offset-Verwaltung und -Zurücksetzen | ✅ | Abfragen/Aktualisieren/Zurücksetzen |

### Protokolle und Kompatibilität

| Serverversion | Remoting | gRPC | Beschreibung |
|----------------|:--------:|:----:|-------------|
| RocketMQ 4.0 ~ 4.9 | ✅ | — | Vollständig kompatibel |
| RocketMQ 5.x (Broker) | ✅ | — | Remoting ist abwärtskompatibel |
| RocketMQ 5.x (Proxy) | — | ✅ | Aktiviert über GrpcProxyAddress |
| Alibaba Cloud 4.x | ✅ | — | AliyunProvider-Adapter |
| Huawei Cloud DMS | ✅ | — | HuaweiProvider-Adapter |
| Tencent Cloud TDMQ | ✅ | — | TencentProvider-Adapter |

---

## Testabdeckung

Die xUnit-Test-Suite deckt Kernfunktionen, erweiterte Funktionen, Protokollkompatibilität, Cloud-Provider-Adapter und E2E-Integrationstests ab (gRPC-Integrationsfälle werden automatisch übersprungen, wenn kein Proxy konfiguriert ist); sie prüft zudem die Authentifizierungssemantik von Apache ACL 2.0 sowohl über Remoting als auch über gRPC (Anfragen ohne Anmeldeinformationen, von Nicht-Administratoren oder mit falschen Schlüsseln werden abgelehnt, während Administratoren normal senden und empfangen können). Unit-Tests laufen parallel je Testklasse, und `dotnet test XUnitTestRocketMQ/XUnitTest.csproj --filter "Category!=Integration"` ist in etwa 4 Sekunden abgeschlossen; Integrationsfälle, die einen echten Broker benötigen, liegen in der RocketMQ-Collection und laufen seriell, um Störungen durch gemeinsam genutzte Topics und Consumer-Gruppen zu vermeiden; setzen Sie `ROCKETMQ_TEST_VERBOSE=1`, um bei der Fehlersuche die Framework-Logs wiederherzustellen. CI läuft als RID-Matrix: Unit-Tests und AOT-Smoke-Tests decken win-x64 / linux-x64 / linux-arm64 / Loongson LA64 (QEMU-Emulation, erforderlich) ab, während win-arm64, beide osx-Architekturen und beide linux-musl-Architekturen in der Beobachtungsphase sind; alles unterstützt die manuelle Auslösung.

Ebenfalls enthalten: NativeAOT-Smoke-Tests (`AotSmoke`, 33 Fälle, einschließlich Nachrichtenkomprimierung, LMQ, ACL-Signierung, gRPC-Pfad, Dead-Letter-Weiterleitung und FIFO-geordneter Verarbeitung), AOT-Smoke-Tests für die Nutzung des NuGet-Pakets (`AotPackageSmoke`, nutzt die gepackten Artefakte über PackageReference und deckt Paketinhalte, quellgenerierte Serialisierung, Komprimierung und End-to-End-Senden/-Empfangen ab), Erfassung einer Publishing-Baseline (`scripts/AotBaseline.cs`, meldet Größe/Startaufwand/Durchsatz mit Schwellenwertprüfungen und schreibt in die Pipeline-Zusammenfassung in CI), AOT/JIT-Leistungsvergleichsbenchmarks (das Projekt `Benchmark` auf Basis von BenchmarkDotNet, einschließlich Allokationsstatistiken; die AOT-Seite verwendet mehrrundige Stichproben über `AotSmoke --bench`), ein Dead-Letter-Werkzeug (`scripts/DlqTool.cs`, veröffentlicht Nachrichten aus `%DLQ%{group}` erneut in einem angegebenen Topic) und ein gRPC-Stabilitätsabnahmeskript (`scripts/GrpcStability.cs`, läuft nachts in CI mit einem injizierten Proxy-Neustart).

### Lokale Integrationstests (Windows)

```powershell
# Installation und Start von NameServer + Broker + Proxy in einem Schritt (der Store wird bei jedem Start geleert)
dotnet run --file scripts/RocketMqSetup.cs

$env:ROCKETMQ_NAMESERVER = "127.0.0.1:9876"
dotnet test XUnitTestRocketMQ\XUnitTest.csproj -c Release

# Stoppen / Status
dotnet run --file scripts/RocketMqSetup.cs -- --stop
dotnet run --file scripts/RocketMqSetup.cs -- --status
```

Der lokale Start verwendet standardmäßig den ressourcenschonenden Modus: Die Logs der drei JVMs werden auf WARN reduziert (nicht mehr nach `%USERPROFILE%\logs\rocketmqlogs` geschrieben), das asynchrone Flush-Intervall beträgt 5 Sekunden, das Commitlog 64 MB, Thread-Pools werden gestrafft, und der Store wird bei jedem Start geleert, sodass der Broker immer in einem frischen Zustand ist. Verwenden Sie bei der Fehlersuche `--log-level INFO` (oder `$env:ROCKETMQ_TEST_LOG_LEVEL="INFO"`), um ausführliche Logs wiederherzustellen (das Protokollvolumen steigt deutlich); wenn gRPC nicht benötigt wird, starten Sie mit `--no-proxy` eine JVM weniger; um den Store auf ein RAM-Laufwerk zu legen, verwenden Sie `--store-dir R:\RocketMQ\store`.

---

## Mitwirken

Issues und Pull Requests sind willkommen!

1. Forken Sie dieses Repository
2. Erstellen Sie einen Feature-Branch (`git checkout -b feature/AmazingFeature`)
3. Committen Sie Ihre Änderungen (`git commit -m 'Add some AmazingFeature'`)
4. Pushen Sie den Branch (`git push origin feature/AmazingFeature`)
5. Öffnen Sie einen Pull Request

---

## Lizenz

Dieses Projekt wird unter der [MIT-Lizenz](LICENSE) veröffentlicht.

---

## Ökosystem-Karte

NewLife ist ein geschichtetes Ökosystem unabhängig versionierter Open-Source-Projekte. Die folgende Karte wird wortgleich in allen NewLife-Repositories verwendet; jedes Projekt verlinkt auf sein Repository und sein primäres NuGet-Paket.

| Projekt | Funktion | Downloads |
| --- | --- | --- |
| **Grundlage** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | Protokollierung, Konfiguration, Caching, Netzwerk, Serialisierung, Zeitplanung und APM-Tracing. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | Daten-Middleware: einzelne Tabellen in der Größenordnung von zig Milliarden Zeilen, automatisches Sharding, Lese-/Schreibtrennung, über 10 Datenbanken. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | Hochperformante Netzwerkbibliothek: 1,77 Mio. pps / 14,5 Gbps auf einer Maschine (TCP 1KB, 2026-10), 4 Mio. langlebige TCP-Verbindungen. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | Protokollkommunikation und RPC-Framework für Client/Server-Anwendungen und IoT-Geräte. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | Installiert .NET-Anwendungen als Betriebssystem-Dienste: Windows-Dienste und Linux-systemd. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | `dotnet new`-Projektvorlagen für Konsole, Web, API, Dienst, Daten und IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **Daten & Messaging** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Redis-Client mit Mikrosekunden-Latenz, Millionen Operationen pro Sekunde und integrierten Nachrichtenwarteschlangen. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Vollständig verwalteter RocketMQ-Client für Apache RocketMQ und Alibaba Cloud Message Queue. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | MQTT-Client und -Server, einschließlich Alibaba Cloud IoT-Anbindung. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | Hybriddatenbank in C#: eingebettet oder im Netzwerk, relational, Zeitreihen, Nachrichtenwarteschlange und KV in einem. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | MySql-Treiber und Hilfsschicht auf Basis des NewLife-Datenstacks. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web, KI & verteilt** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | Schnelles Framework für Administrationsplattformen: Berechtigungen, SSO, OAuth-Server und CRUD-Gerüst. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | Verteilte Dienstplattform: Knotenverwaltung, APM, Konfigurationszentrum, Registry und Release-Center. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | Plattform für verteilte Aufgaben und Echtzeit-Datenplanung mit Sharding, Wiederholungen und Backfill. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | Einheitlicher Client für 46 LLM-Anbieter plus ChatAI, eine sofort einsatzbereite Web-Chat-Anwendung. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **Industrie & IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | IoT-Standardbibliothek mit Protokollverträgen und Geräteabstraktionen. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | Modbus-TCP/RTU/ASCII-Implementierung auf Basis von NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | Siemens-SPS-Protokoll auf Basis von NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | Omron-SPS-Protokoll auf Basis von NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | Mitsubishi-Melsec-SPS-Protokoll auf Basis von NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | OPC-Client für industriellen Datenzugriff. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | BACnet-Protokollstack für die Gebäudeautomation. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | Leichtgewichtige Open-Source-IoT-Plattform mit Gateway- und Geräteverwaltung. | — |
| **Fahrzeugtelematik** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | JT/T-808-Fahrzeugortungsprotokoll: über 70 Nachrichtenkörper, 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | JT/T-809-Plattform-zu-Plattform-Protokoll für Verkehrsbehörden. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | JT/T-1078-Fahrzeugvideo: RTP-Parsing, FLV/fMP4-Remuxing und Live-Streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **Domänenkomponenten** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Ein Paket für Excel, Word, PPT, PDF und über 15 Dokumentformate. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | Plattformübergreifendes Audio: Codecs, Geräte-I/O, DSP, Container und Streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | Videocodecs, Containerformate, Frame-Verarbeitung und Streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | Einheitliche Schnittstelle für Baidu, AMap, Tencent und Tianditu mit Geokodierung und Koordinatenumrechnung. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | IPv4-Geolokalisierung mit einer offline speichergemappten Datenbank und einer Online-API als Fallback. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **Werkzeuge & Produkte** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | Entwickler-Toolbox: Netzwerk, serielle Schnittstelle, Krypto, Regex, Modbus und MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | Selbst gehostetes Git-Quellcode-Verwaltungssystem. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | Installiert automatisch die passende .NET-Laufzeit, wenn es von einem älteren Host gestartet wird. | — |

> Die Badges zeigen das primäre Paket jedes Projekts; Theme-, Erweiterungs- und Satellitenpakete sind auf den jeweiligen NuGet-Seiten aufgeführt. Die hier nicht aufgeführten über 30 Pakete (FTP, LoRa, Office-Erweiterungen, Protokolltreiber und mehr) sind in der [NewLifeX-Organisation](https://github.com/NewLifeX) und auf [newlifex.com](https://newlifex.com) verfügbar.

## Team

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

NewLife wurde 2002 gegründet und widmet sich seit mehr als zwei Jahrzehnten dem .NET- und IoT-Ökosystem, betreut fast 100 Open-Source-Projekte (34 davon Hauptprojekte) und liefert integrierte Hard- und Softwarelösungen für die Bereiche Energie, Logistik, Gesundheitswesen, Industrieautomation und Internet. Unser Leitspruch lautet „Lernen kennt kein Dienstalter – wer es beherrscht, wird zum Lehrmeister“.

Die Open-Source-Komponenten wurden <!--nuget-total:start-->6.48M<!--nuget-total:end--> Mal von NuGet heruntergeladen (ökosystemweit, Stand 2026-10).

Website: https://newlifex.com · Open-Source-Organisation: https://github.com/NewLifeX

<details>
<summary>WeChat-Offizieller Account: 智能大石头 (zum Folgen scannen)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
