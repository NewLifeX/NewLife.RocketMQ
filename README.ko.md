# NewLife.RocketMQ - 엔터프라이즈급 완전 관리형 RocketMQ 클라이언트

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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [العربية](README.ar.md)
<!-- lang:end -->

> 이 번역은 [영어 버전](README.md)보다 늦을 수 있습니다.

**`.NET Framework 4.5+` / `.NET Standard 2.0+` / `.NET Core` / `.NET 5+`를 지원하는 엔터프라이즈급 완전 관리형 RocketMQ 클라이언트입니다.**  
**전체를 순수 C#로 구현했으며 외부 의존성이 전혀 없습니다(Java, gRPC, Protobuf 서드파티 라이브러리 불필요).**

---

## 제품 개요

NewLife.RocketMQ는 뉴라이프 팀이 .NET 생태계를 위해 개발한 **엔터프라이즈급 완전 관리형 RocketMQ 클라이언트**입니다. RocketMQ **Remoting 프로토콜(4.x/5.x Broker)** 과 **gRPC Proxy 프로토콜(5.x Proxy)** 을 모두 지원하며, 프로듀서와 컨슈머의 핵심 능력 및 엔터프라이즈 기능을 전부 다루고, 알리바바 클라우드·화웨이 클라우드·텐센트 클라우드와 Apache ACL 인증 체계를 위한 통일된 어댑터를 제공합니다.

**핵심 장점**:

| 기능 | 설명 |
|---------|-------------|
| **이중 프로토콜 지원** | Remoting(4.x에서 성숙하고 안정적) + gRPC(5.x의 미래 지향), 자동 라우팅 |
| **외부 의존성 제로** | 내장 Protobuf 필드 코덱(NewLife.Core의 SpanWriter/SpanReader 위에 직접 구현), Java나 gRPC 런타임 불필요 |
| **멀티 클라우드 지원** | 통일된 `ICloudProvider` 인터페이스와 알리바바 클라우드·화웨이 클라우드·텐센트 클라우드·Apache ACL 내장 어댑터 |
| **프로덕션 레디** | 소비 재시도, 데드 레터 큐, 트랜잭션 검사백, 순서 소비, Pop 소비 등 완전한 엔터프라이즈 기능 |
| **이벤트 버스** | Core `IEventBus` 계약 기반의 내장 이벤트 버스: Producer/Consumer를 통해 프로세스 간에 이벤트를 발행·구독하며, 실패 시 자동 재시도/데드 레터 처리 |
| **폭넓은 프레임워크 지원** | .NET Framework 4.5+부터 .NET 10까지; gRPC 기능은 .NET Standard 2.1+에서 사용 가능 |
| **AOT 친화** | net10.0 타깃은 NativeAOT/트리밍을 지원: 프레임 인코딩/디코딩, JSON 직렬화, 메시지 압축이 런타임 리플렉션에 의존하지 않음. 저장소에 AOT 스모크 테스트와 NuGet 패키지 소비 AOT 스모크 테스트가 포함되며, CI가 주요 RID 매트릭스(win/linux/osx, musl 및 Loongson LA64)에서 AOT 게시와 실행을 검증 |
| **런타임 현지화** | 로그, 프로토콜 파싱 예외, 공개 API 예외, 구성 설명이 영어를 원문으로 하며, 31개 언어 내장 단어 목록을 통해 `Localization.Culture`에 따라 자동 번역되고 영어로 폴백; 접두사가 붙은 로그는 접두사 조합 전에 번역되며 접두사는 단어 목록 조회에 참여하지 않음 |
| **고성능** | NewLife.Net 고성능 네트워크 계층 기반: 연결 재사용, VIP 채널, 메시지 압축, 동시성 제어 |

---

## 설치

```powershell
# NuGet 패키지 관리자
Install-Package NewLife.RocketMQ

# .NET CLI
dotnet add package NewLife.RocketMQ
```

```xml
<!-- PackageReference -->
<PackageReference Include="NewLife.RocketMQ" Version="4.0.*" />
```

---

## 빠른 시작

### 메시지 전송

```csharp
using NewLife.RocketMQ;

var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    Group = "producer_group"
};
producer.Start();

// 동기 전송
var result = producer.Publish("Hello RocketMQ!");
Console.WriteLine($"Message ID: {result.MsgId}");

// 비동기 전송
await producer.PublishAsync("async message");

// 배치 전송
await producer.PublishBatch(new[] { "message1", "message2", "message3" });
```

### 메시지 소비

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
    return true; // true를 반환하면 소비 성공을 의미합니다
};

consumer.Start();
```

### gRPC(5.x Proxy) 접속

```csharp
// Producer: GrpcProxyAddress를 설정하면 5.x gRPC 경로로 트래픽이 라우팅됩니다(netstandard2.1+ 필요)
var producer = new Producer
{
    Topic = "test_topic",
    Group = "producer_group",
    NameServerAddress = "127.0.0.1:9876",
    GrpcProxyAddress = "http://127.0.0.1:8081",
};
producer.Start();
await producer.PublishAsync("gRPC message");

// Consumer: 시작 시 구독 관계가 자동으로 보고되고(Telemetry 장기 연결), 수신과 확인 응답은 gRPC로 수행됩니다
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
// ... 비즈니스 처리 ...
await consumer.AckMessageViaGrpcAsync("test_topic",
[
    .. messages.Select(m => new AckMessageEntry
    {
        ReceiptHandle = m.SystemProperties?.ReceiptHandle,
        MessageId = m.SystemProperties?.MessageId,
    }),
]);
```

> 장폴링 소비는 `GrpcPushConsumer`를 직접 사용할 수도 있습니다(PUSH_CONSUMER 시맨틱, 자동 Ack와 재시도 내장).
>
> **5.x POP 재시도와 데드 레터**: 소비에 실패한 메시지는 Broker가 `%RETRY%{group}_{topic}`에 기록하고 `popFromRetryProbability`에 따라 원래 토픽의 풀 응답에 다시 병합하므로, 클라이언트는 원래 토픽만 소비하면 됩니다. 전달 횟수가 `GrpcPushConsumer.MaxDeliveryAttempts`(기본 16)에 도달하면 메시지는 `%DLQ%{group}`으로 이동합니다. 수동 수신 시나리오에서는 `Consumer.ForwardToDeadLetterQueueViaGrpcAsync`를 호출합니다(`GetGrpcDlqTopic()`은 데드 레터 토픽 이름을 반환). 보이지 않는 시간(invisible time)의 하한은 10초이며(라이브러리 내부에서 자동 수렴), 더 작은 값은 Broker가 거부합니다.
>
> 데드 레터 메시지는 한 번에 처리해야 합니다: `dotnet run --file scripts/DlqTool.cs -- --group <consumer group> --to <original topic>`은 데드 레터 큐의 메시지를 지정한 토픽으로 재게시하고 재게시에 성공한 메시지만 확인 응답합니다. 데드 레터 메시지를 풀하면 소비 오프셋이 전진하고, 확인 응답하지 않은 메시지는 Broker가 `%RETRY%{group}_%DLQ%{group}`으로 이동시키므로 데드 레터 큐의 읽기 전용 탐색은 지원되지 않습니다.
>
> **FIFO 순서 메시지와 트랜잭션 메시지**: 토픽 유형은 메시지 유형과 일치해야 하며, `CreateTopic(topic, queueNum, 0, TopicMessageType.Fifo/Transaction)`으로 생성합니다. FIFO 전송 시 `Message.MessageGroup`을 설정하고, 소비 측에서는 `GrpcPushConsumer.Orderly = true`를 설정하면 직렬 처리로 순서를 보존합니다. 트랜잭션 메시지는 `PublishTransactionViaGrpcAsync`로 하프 메시지를 보내고 `EndTransactionViaGrpcAsync`로 커밋하거나 롤백합니다(커밋하지 않으면 Broker가 타임아웃 후 롤백).

### 지연 메시지

```csharp
// 18개 사전 설정 지연 레벨
producer.PublishDelay("delayed message", DelayTimeLevels.s30);

// gRPC 모드는 임의 지연을 지원(netstandard2.1+ 필요)
producer.GrpcProxyAddress = "http://127.0.0.1:8081";
await producer.PublishDelayViaGrpcAsync("arbitrary delay", DateTime.Now.AddMinutes(30));
```

### 트랜잭션 메시지

```csharp
var producer = new Producer
{
    Topic = "tx_topic",
    Group = "tx_group",
    NameServerAddress = "127.0.0.1:9876"
};

// 트랜잭션 검사백 콜백
producer.OnCheckTransaction = (msg, transactionId) =>
{
    var success = CheckLocalTransaction(transactionId);
    return success ? TransactionState.Commit : TransactionState.Rollback;
};
producer.Start();

// 하프 메시지 전송 -> 로컬 트랜잭션 실행 -> 커밋/롤백
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

### 순서 메시지

```csharp
// 동일한 키를 가진 메시지는 같은 큐로 전달됩니다
var queue = producer.SelectQueue("order_123");
producer.Publish("ordered message 1", queue);
producer.Publish("ordered message 2", queue);

// 소비 측에서 순서 소비 활성화
consumer.OrderConsume = true;
```

### 요청-응답 패턴

```csharp
// Producer가 요청 전송(동기/비동기)
var response = producer.Request("request message", timeout: 5000);
var reply = await producer.RequestAsync("async request", timeout: 5000);

// Consumer가 응답
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

### 이중 프로토콜 메시지 변환

```csharp
// Remoting 메시지를 gRPC 메시지로 변환(netstandard2.1+ 필요)
var grpcMsg = message.ToGrpcMessage();

// gRPC 메시지를 확장 Remoting 메시지 / 기본 메시지로 변환
var ext = grpcMsg.ToMessageExt();   // MessageExt (모든 시스템 속성 포함)
var msg = grpcMsg.ToMessage();      // Message (기본 필드)
```

---

### 이벤트 버스

```csharp
using NewLife.Messaging;
using NewLife.RocketMQ;

// 이벤트 토픽에는 점을 포함할 수 있으며, 이름은 RocketMQ 유효 이름으로 정규화됩니다(order.created -> order_created)
using var bus = new RocketMqEventBus<OrderPaidEvent>("127.0.0.1:9876", "order.created", "order_service");

// 구독: 처리기가 예외를 던지면 재전송되고, 한도를 넘으면 데드 레터 큐로 이동합니다
await bus.SubscribeAsync((OrderPaidEvent e, IEventContext ctx) =>
{
    Console.WriteLine($"주문 결제 완료: {e.OrderId}");
    return Task.CompletedTask;
}, "billing");

// 발행: Broker를 통해 전달되며, 로컬 구독자도 소비자 그룹을 통해 수신합니다
await bus.PublishAsync(new OrderPaidEvent { OrderId = 1001 });

// 이벤트 허브 연결: 팩토리가 토픽별로 버스를 생성하고 캐시합니다
var hub = new EventHub<OrderPaidEvent>
{
    Factory = new RocketMqEventBusFactory("127.0.0.1:9876"),
};
```

> 소비자 그룹은 기본적으로 현재 노드 식별자를 사용하므로 모든 노드가 각 이벤트를 받습니다. 동일한 clientId를 전달하면 경쟁 소비자 의미로 전환됩니다. 배치 일부가 실패하면 전체 배치가 재전송되므로 처리기는 멱등성을 유지해야 합니다.

---

## Consumer 고급 기능

### 소비 재시도와 데드 레터 큐

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    EnableRetry = true,           // 소비 재시도 활성화
    MaxReconsumeTimes = 3         // 최대 재시도 횟수, 초과한 메시지는 %DLQ% 데드 레터 큐로 이동
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        try { ProcessMessage(msg); }
        catch { return false; } // false를 반환하면 재시도가 트리거됩니다
    }
    return true;
};
```

### 태그 / SQL92 필터링

```csharp
// 태그 필터링
consumer.Tags = "TagA || TagB";

// SQL92 표현식 필터링
consumer.ExpressionType = "SQL92";
consumer.Subscription = "age > 18 AND city = 'Shanghai'";
```

### 멀티 토픽 구독

```csharp
consumer.Topics = "topic1;topic2;topic3";
```

### Pop 소비 모드

```csharp
// Pop 소비(수동 확인 응답)
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
        await consumer.ChangeInvisibleTimeAsync(msg, 30000); // 처리 시간 연장
    }
}
```

### 소비 스로틀링 / VIP 채널 / 메시지 압축

```csharp
consumer.MaxConcurrentConsume = 10; // 최대 10개 메시지를 동시에 처리

producer.VipChannelEnabled = true;  // VIP 채널 활성화(BrokerPort - 2)
producer.CompressOverBytes = 4096;  // 4KB를 초과하는 메시지 바디를 자동 ZLIB 압축
```

### HTTP 브리지(MQ -> WebAPI)

소비한 메시지를 WebAPI로 전달하거나, 외부에서 메시지를 게시할 수 있도록 WebAPI를 노출하려면 기존 API를 조합하면 됩니다. 자세한 내용은 [HTTP 브리지 가이드](Doc/HttpBridge_Guide.md)를 참고하세요.

---

## 클라우드 프로바이더 접속

### 알리바바 클라우드 ApsaraMQ for RocketMQ

```csharp
var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "http://MQ_INST_xxx.aliyuncs.com:80",
    CloudProvider = new AliyunProvider
    {
        AccessKey = "your AccessKey",
        SecretKey = "your SecretKey",
        InstanceId = "MQ_INST_xxx"  // 선택 사항, 주소에서 자동 파싱
    }
};
```

### 화웨이 클라우드 DMS for RocketMQ

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

### 텐센트 클라우드 TDMQ RocketMQ

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

### Apache RocketMQ ACL 인증

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

## NativeAOT 게시

`net10.0` 타깃만 NativeAOT/트리밍을 지원합니다(이전 타깃 프레임워크는 트리밍 개념이 없어 동작이 동일합니다). 패키지에는 이미 소스 생성 멤버 접근자와 메타데이터 루트가 포함되어 있으므로 사용 시 별도 구성이 필요하지 않습니다:

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

검증된 시나리오(네이티브 실행 파일로 측정): 프레임 인코딩/디코딩, 요청 헤더 속성 사전, JSON 직렬화, 메시지 압축(ZLIB 왕복과 압축 해제 한도), LMQ 속성, ACL 서명, 클라우드 프로바이더 어댑터, 이중 프로토콜 변환, 엔드투엔드 송수신(일반/객체 바디/배치/지연/대용량 메시지 압축, 소비, 오프셋 조회, 순서 잠금, POP) 및 gRPC 5.x 경로(라우팅, 송수신 확인 응답, 푸시 소비, 데드 레터 전달, FIFO 순서 소비).

CI 커버리지: win-x64(로컬 RocketMQ 전체 엔드투엔드 포함), linux-x64, linux-arm64, win-arm64, osx-x64/osx-arm64, linux-musl-x64/arm64는 각각 네이티브로 게시·실행하며, Loongson linux-loongarch64는 QEMU 사용자 모드 에뮬레이션과 Loongson SDK로 실행합니다(해당 플랫폼은 현재 공식 NativeAOT 지원이 없으므로 AOT 스타일 엔지니어링 JIT 스모크 테스트를 기준으로 삼습니다).

**사용자 정의 타입 객체 바디는 반드시 등록해야 합니다**: 사용자 정의 타입(POCO)을 메시지 바디로 사용하면 트리머가 기본적으로 해당 public 속성을 제거하여 객체 바디가 조용히 빈 객체 `{}`로 직렬화됩니다. 애플리케이션 시작 시 메시지 바디로 사용하는 각 타입을 한 번 등록하세요:

```csharp
MqAot.RegisterBodyType<MyOrder>();
MqAot.RegisterBodyType<MyEvent>();
```

`MqAot.RegisterBodyType<T>()`는 net10.0에서만 트리밍 보존 의미를 가지며 다른 타깃에서는 no-op이므로 멀티 타깃 프로젝트가 동일한 시작 코드를 공유할 수 있습니다. 라이브러리와 같은 방식(`[DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MyOrder))]` + `[ModuleInitializer]`)으로 자체 메타데이터 루트를 선언할 수도 있습니다.

---

## 아키텍처 개요

```
MqBase (비즈니스 기본 클래스)
├── Producer
└── Consumer

통신 계층
├── Remoting 프로토콜 (4.x/5.x Broker)
│   ├── ClusterClient (TCP 장기 연결, Opaque 재사용)
│   ├── NameClient (라우트 검색, 30초 폴링)
│   └── BrokerClient (하트비트/등록 해제)
│
└── gRPC 프로토콜 (5.x Proxy, netstandard2.1+)
    ├── GrpcClient (HTTP/2, Unary + Server Streaming + 쓰기 전용 장기 연결)
    ├── GrpcMessagingService (11개 RPC 메서드)
    ├── GrpcTelemetrySession (리소스 보고 장기 연결: 주기적 재전송 + 연결 끊김 시 재연결)
    ├── MessageConverter (Remoting ↔ gRPC 메시지 변환)
    └── 필드 코덱 (SpanWriter/SpanReader 기반으로 직접 구현한 Protobuf)

클라우드 프로바이더 어댑터 계층
├── AliyunProvider  (알리바바 클라우드: 인스턴스 ID 라우팅 + HTTP NameServer)
├── HuaweiProvider  (화웨이 클라우드: SSL/TLS + 인스턴스 ID 라우팅)
├── TencentProvider (텐센트 클라우드: Namespace 접두사 라우팅)
└── AclProvider     (Apache ACL: HMAC-SHA1 서명)
```

[요구사항 문서](Doc/需求文档.md), [기능 목록](Doc/功能清单.md), [아키텍처 설계](Doc/架构设计.md), [경쟁 분석](Doc/竞品分析.md), [HTTP 브리지 가이드](Doc/HttpBridge_Guide.md), [현지화](Doc/多语言本地化.md)를 참고하세요.

---

## 기능 개요

### Producer

| 기능 | 상태 | 설명 |
|---------|:------:|-------------|
| 동기/비동기/단방향 전송 | ✅ | Publish / PublishAsync / PublishOneway |
| 배치 메시지 전송 | ✅ | PublishBatch, 여러 메시지를 하나의 요청으로 병합 |
| 지연 메시지 | ✅ | 18개 사전 설정 레벨 + gRPC를 통한 임의 지연 |
| 트랜잭션 메시지 | ✅ | 하프 메시지 + 커밋/롤백 + 검사백 콜백 |
| 순서 메시지 | ✅ | 지정된 MessageQueue로 전송 |
| 요청-응답 | ✅ | 동기/비동기 요청-응답 |
| 메시지 압축 | ✅ | CompressOverBytes 임계값 초과 시 자동 ZLIB |
| 메시지 트레이스 | ✅ | AsyncTraceDispatcher + MessageTraceHook |

### Consumer

| 기능 | 상태 | 설명 |
|---------|:------:|-------------|
| 풀 소비 / 소비 스케줄링 | ✅ | 장폴링 풀 + 자동 큐 할당 |
| 클러스터/브로드캐스트 소비 | ✅ | Rebalance 평균 할당 / 로컬 오프셋 영속화 |
| 태그 / SQL92 필터링 | ✅ | 표현식 필터링 |
| 멀티 토픽 구독 | ✅ | Topics 속성; 토픽별 Rebalance |
| 소비 재시도 + 데드 레터 큐 | ✅ | EnableRetry + MaxReconsumeTimes |
| 순서 소비 | ✅ | 큐 잠금(OrderConsume) |
| Pop 소비 | ✅ | Pop/Ack/BatchAck/ChangeInvisibleTime |
| 소비 스로틀링 | ✅ | MaxConcurrentConsume 세마포어 제어 |

### 이벤트 버스

| 기능 | 상태 | 설명 |
|------|:----:|------|
| Core IEventBus 계약 | ✅ | RocketMqEventBus&lt;TEvent&gt; / RocketMqEventContext |
| 프로세스 간 게시/구독 | ✅ | Producer가 발행하고 Consumer 소비자 그룹이 구독; 실패는 재시도/데드 레터로 |
| 이벤트 허브 팩토리 | ✅ | RocketMqEventBusFactory가 IEventBusFactory를 구현하며 EventHub.Factory에 연결 |
| 이름 정규화 | ✅ | MqEventBusNaming: 잘못된 문자를 밑줄로, 127자 절단 |

### 관리 및 운영

| 기능 | 상태 | 설명 |
|---------|:------:|-------------|
| 토픽/컨슈머 그룹 CRUD | ✅ | 생성/수정/삭제 |
| 메시지 조회 | ✅ | ID 기준 / Key 기준 |
| 소비 통계 / 클러스터 정보 | ✅ | GetConsumeStats / GetClusterInfo |
| 오프셋 관리와 리셋 | ✅ | 조회/업데이트/리셋 |

### 프로토콜 및 호환성

| 서버 버전 | Remoting | gRPC | 설명 |
|----------------|:--------:|:----:|-------------|
| RocketMQ 4.0 ~ 4.9 | ✅ | — | 완전 호환 |
| RocketMQ 5.x (Broker) | ✅ | — | Remoting 하위 호환 |
| RocketMQ 5.x (Proxy) | — | ✅ | GrpcProxyAddress로 활성화 |
| 알리바바 클라우드 4.x | ✅ | — | AliyunProvider 어댑터 |
| 화웨이 클라우드 DMS | ✅ | — | HuaweiProvider 어댑터 |
| 텐센트 클라우드 TDMQ | ✅ | — | TencentProvider 어댑터 |

---

## 테스트 커버리지

xUnit 테스트 스위트는 핵심 기능, 고급 기능, 프로토콜 호환성, 클라우드 프로바이더 어댑터, E2E 통합 테스트를 다룹니다(Proxy가 구성되지 않으면 gRPC 통합 케이스는 자동으로 건너뜁니다). 또한 Remoting과 gRPC 양쪽에서 Apache ACL 2.0의 인증 시맨틱을 검증합니다(자격 증명이 없는 요청, 비관리자 사용자, 잘못된 키를 사용한 요청은 거부되고 관리자는 정상적으로 송수신). 단위 테스트는 테스트 클래스 단위로 병렬 실행되며 `dotnet test XUnitTestRocketMQ/XUnitTest.csproj --filter "Category!=Integration"`은 약 4초에 완료됩니다. 실제 Broker에 의존하는 통합 케이스는 RocketMQ 컬렉션에 배치되어 공유 토픽과 컨슈머 그룹의 간섭을 피하기 위해 직렬 실행됩니다. 문제 해결 시 프레임워크 로그를 복원하려면 `ROCKETMQ_TEST_VERBOSE=1`을 설정하세요. CI는 RID 매트릭스로 실행됩니다: 단위 테스트와 AOT 스모크 테스트는 win-x64 / linux-x64 / linux-arm64 / Loongson LA64(QEMU 에뮬레이션, 필수)를 커버하고, win-arm64, osx 두 아키텍처, linux-musl 두 아키텍처는 관찰 기간 중입니다. 모두 수동 트리거를 지원합니다.

또한 다음이 포함됩니다: NativeAOT 스모크 테스트(`AotSmoke`. 메시지 압축, LMQ, ACL 서명, gRPC 경로, 데드 레터 전달, FIFO 순서 소비 포함), NuGet 패키지 소비 측 AOT 스모크 테스트(`AotPackageSmoke`. PackageReference로 패키징된 산출물을 소비하며 패키지 자산, 소스 생성 직렬화, 압축, 엔드투엔드 송수신을 커버), 게시 베이스라인 수집(`scripts/AotBaseline.cs`. 크기/시작 오버헤드/처리량을 보고하고 임계값 검사 및 CI 파이프라인 요약 기록), AOT/JIT 성능 비교 벤치마크(BenchmarkDotNet 기반 `Benchmark` 프로젝트, 할당 통계 포함. AOT 측은 `AotSmoke --bench`로 다회 샘플링), 데드 레터 운영 도구(`scripts/DlqTool.cs`. `%DLQ%{group}` 메시지를 지정 토픽으로 재게시), gRPC 안정성 수용 스크립트(`scripts/GrpcStability.cs`. CI에서 매일 밤 실행하며 Proxy 재시작 1회를 주입).

### 로컬 통합 테스트(Windows)

```powershell
# NameServer + Broker + Proxy 원스텝 설치 및 시작(스토어는 매번 초기화됩니다)
dotnet run --file scripts/RocketMqSetup.cs

$env:ROCKETMQ_NAMESERVER = "127.0.0.1:9876"
dotnet test XUnitTestRocketMQ\XUnitTest.csproj -c Release

# 중지 / 상태 확인
dotnet run --file scripts/RocketMqSetup.cs -- --stop
dotnet run --file scripts/RocketMqSetup.cs -- --status
```

로컬 기동은 기본적으로 저자원 모드를 사용합니다. 세 개 JVM의 로그는 WARN으로 낮추고(`%USERPROFILE%\logs\rocketmqlogs`에 기록하지 않음), 비동기 플러시 간격은 5초, commitlog는 64MB, 스레드 풀은 축소되며, 스토어는 매번 초기화되어 Broker가 항상 새 상태입니다. 문제 해결 시에는 `--log-level INFO`(또는 `$env:ROCKETMQ_TEST_LOG_LEVEL="INFO"`)로 상세 로그를 복원할 수 있습니다(로그 양이 눈에 띄게 늘어납니다). gRPC가 필요 없으면 `--no-proxy`로 JVM을 하나 줄여 시작하고, 스토어를 RAM 디스크에 두려면 `--store-dir R:\RocketMQ\store`를 사용하세요.

---

## 기여

이슈와 Pull Request를 환영합니다!

1. 이 저장소를 Fork합니다
2. 기능 브랜치를 만듭니다 (`git checkout -b feature/AmazingFeature`)
3. 변경 사항을 커밋합니다 (`git commit -m 'Add some AmazingFeature'`)
4. 브랜치에 푸시합니다 (`git push origin feature/AmazingFeature`)
5. Pull Request를 엽니다

---

## 라이선스

이 프로젝트는 [MIT License](LICENSE)에 따라 배포됩니다.

---

## 생태계 지도

NewLife는 독립적으로 버전이 관리되는 오픈소스 프로젝트의 계층형 생태계입니다. 아래 지도는 모든 NewLife 리포지토리에서 그대로 동일하게 공유되며, 각 프로젝트는 코드 리포지토리와 기본 NuGet 패키지로 연결됩니다.

| 프로젝트 | 한 줄 소개 | 다운로드 |
| --- | --- | --- |
| **기반** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | 로깅, 구성, 캐시, 네트워크, 직렬화, 스케줄링, APM 추적. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | 데이터 미들웨어: 단일 테이블 수백억 건, 자동 분할 테이블, 읽기/쓰기 분리, 10여 종 데이터베이스 지원. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | 고성능 네트워크 라이브러리: 단일 노드 177만 pps, 14.5 Gbps(TCP 1KB, 2026-10 실측), 400만 Tcp 장기 연결. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | CS 애플리케이션과 IoT 장치를 위한 프로토콜 통신 및 RPC 프레임워크. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | .NET 애플리케이션을 운영체제 데몬으로 설치: Windows 서비스와 Linux systemd. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | 콘솔, Web, API, 서비스, 데이터, IoT용 `dotnet new` 프로젝트 템플릿. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **데이터 및 메시징** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Redis 클라이언트: 마이크로초 수준 지연, 백만 수준 처리량, 메시지 큐 내장. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Apache RocketMQ와 알리바바 클라우드 메시지 큐를 지원하는 순수 관리형 RocketMQ 클라이언트. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | MQTT 클라이언트와 서버, 알리바바 클라우드 IoT 연결 포함. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | C# 하이브리드 데이터베이스: 임베디드/네트워크 두 모드, 관계형, 시계열, 메시지 큐, KV를 하나로. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | NewLife 데이터 스택 위에 구축한 MySql 드라이버와 도우미 계층. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web, AI 및 분산** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | 빠른 관리 플랫폼 프레임워크: 권한, SSO, OAuth 서버, CRUD 스캐폴딩. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | 분산 서비스 플랫폼: 노드 관리, APM, 구성 센터, 등록 센터, 배포 센터. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | 샤딩, 재시도, 백필을 지원하는 분산 작업 및 실시간 데이터 스케줄링 플랫폼. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | 46개 LLM 공급자를 통합 연결하는 클라이언트와 바로 사용할 수 있는 ChatAI 웹 채팅 애플리케이션. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **산업 및 IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | 프로토콜 계약과 장치 추상을 정의하는 IoT 표준 라이브러리. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | NewLife.IoT 기반으로 구현한 Modbus TCP/RTU/ASCII. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | NewLife.IoT 기반으로 구현한 지멘스 PLC 프로토콜. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | NewLife.IoT 기반으로 구현한 오므론 PLC 프로토콜. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | NewLife.IoT 기반으로 구현한 미쓰비시 Melsec PLC 프로토콜. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | 산업 데이터 수집용 OPC 클라이언트. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | BACnet 빌딩 자동화 프로토콜 스택. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | 게이트웨이와 장치 관리를 갖춘 경량 오픈소스 IoT 플랫폼. | — |
| **차량 텔레매틱스** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | JT/T 808 차량 위치 프로토콜: 70개 이상 메시지 본문, 2011/2013/2019 지원. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | 운송 관리 부서를 위한 JT/T 809 플랫폼 간 프로토콜. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | JT/T 1078 차량용 비디오: RTP 파싱, FLV/fMP4 리먹싱, 라이브 스트리밍. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **도메인 컴포넌트** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Excel, Word, PPT, PDF 등 15개 이상 문서 형식을 하나의 패키지로. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | 크로스 플랫폼 오디오: 코덱, 장치 IO, DSP, 컨테이너, 스트리밍. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | 비디오 코덱, 컨테이너 형식, 프레임 처리, 스트리밍. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | 바이두/고덕/텐센트/톈디투 지도 통합 인터페이스, 지오코딩과 좌표 변환 지원. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | IPv4 위치 정보: 오프라인 메모리 매핑 데이터베이스와 온라인 API 이중 소스. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **도구 및 제품** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | 개발자 도구 모음: 네트워크, 시리얼 포트, 암호화, 정규식, Modbus, MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | 셀프 호스팅 Git 소스 코드 관리 시스템. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | 구버전 호스트에서 시작할 때 일치하는 .NET 런타임을 자동으로 설치합니다. | — |

> 배지는 각 프로젝트의 기본 패키지를 나타냅니다. 테마, 확장, 위성 패키지는 해당 NuGet 페이지에서 확인하세요. 여기에 나열하지 않은 30여 개 패키지(FTP, LoRa, Office 확장, 각종 프로토콜 드라이버 등)는 [NewLifeX 조직](https://github.com/NewLifeX)과 [newlifex.com](https://newlifex.com)에서 확인할 수 있습니다.

---

## 뉴라이프 개발팀

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

뉴라이프 팀(NewLife)은 2002년에 설립되어 20여 년간 .NET과 IoT 기술 생태계에 집중하며 100여 개의 오픈소스 프로젝트를 주도하고(이 중 34개가 핵심 프로젝트), 전력, 물류, 의료, 산업 제어, 인터넷 등 산업에 소프트웨어와 하드웨어를 아우르는 통합 솔루션을 제공해 왔습니다. "학문에는 선후가 없으며, 이룬 사람이 스승이다"라는 신조를 따릅니다.

오픈소스 컴포넌트는 NuGet에서 누적 <!--nuget-total:start-->6.48M<!--nuget-total:end-->회 다운로드되었습니다(생태계 전체 기준, 2026-10).

웹사이트: https://newlifex.com · 오픈소스 조직: https://github.com/NewLifeX

<details>
<summary>위챗 공식 계정: 智能大石头(스캔하여 팔로우)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
