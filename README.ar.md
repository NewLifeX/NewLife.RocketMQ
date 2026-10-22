# NewLife.RocketMQ - عميل RocketMQ مؤسسي مُدار بالكامل

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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md)
<!-- lang:end -->

> قد تتأخر هذه الترجمة عن [English README](README.md).

**عميل RocketMQ مؤسسي مُدار بالكامل** يدعم `.NET Framework 4.5+` / `.NET Standard 2.0+` / `.NET Core` / `.NET 5+`.  
**مُنفَّذ بالكامل بلغة C#، بصفر اعتماديات خارجية (لا حاجة إلى Java أو gRPC أو مكتبات Protobuf من طرف ثالث).**

---

## نظرة عامة على المنتج

NewLife.RocketMQ هو **عميل RocketMQ مؤسسي مُدار بالكامل** طوّره فريق NewLife وصُمّم لمنظومة .NET. يدعم كلاً من **بروتوكول Remoting (Broker 4.x/5.x)** و**بروتوكول gRPC Proxy (Proxy 5.x)**، ويغطي جميع قدرات المنتج والمستهلك الأساسية إلى جانب الميزات المؤسسية، مع محوّلات موحّدة لـ Alibaba Cloud وHuawei Cloud وTencent Cloud ونظام مصادقة Apache ACL.

**المزايا الأساسية**:

| الميزة | الوصف |
|---------|-------------|
| **دعم بروتوكولين** | Remoting (ناضج ومستقر على 4.x) + gRPC (موجّه للمستقبل على 5.x)، مع توجيه تلقائي |
| **صفر اعتماديات خارجية** | مرمّز حقول Protobuf مدمج (مكتوب يدويًا على SpanWriter/SpanReader في NewLife.Core)، دون الحاجة إلى Java أو بيئة تشغيل gRPC |
| **دعم سحابات متعددة** | واجهة `ICloudProvider` موحّدة مع محوّلات مدمجة لـ Alibaba Cloud وHuawei Cloud وTencent Cloud وApache ACL |
| **جاهز للإنتاج** | ميزات مؤسسية كاملة: إعادة محاولة الاستهلاك، وقائمة الرسائل الميتة، ومراجعة المعاملات، والاستهلاك المرتّب، واستهلاك Pop وغير ذلك |
| **أوسع تغطية للأُطر** | من .NET Framework 4.5+ حتى .NET 10؛ وميزات gRPC متاحة على .NET Standard 2.1+ |
| **ملائم لـ AOT** | هدف `net10.0` يدعم NativeAOT/trimming: ترميز الإطارات وفك ترميزها، وتسلسل JSON، وضغط الرسائل لا تعتمد على الانعكاس وقت التشغيل. يتضمّن المستودع اختبار دخان AOT واختبار دخان AOT لاستهلاك حزمة NuGet؛ ويتحقق CI من نشر AOT وتشغيله على مصفوفة RID رئيسية (win/linux/osx، وmusl، وLoongson LA64) |
| **توطين وقت التشغيل** | السجلات، واستثناءات تحليل البروتوكول، واستثناءات واجهات API العامة، وأوصاف الإعدادات تستخدم الإنجليزية كنص مصدر، مع قوائم كلمات مدمجة لـ31 لغة تُترجَم تلقائيًا وفق `Localization.Culture` مع الرجوع إلى الإنجليزية؛ وتُترجَم السجلات ذات البادئة قبل تركيب البادئة، ولا تشارك البادئة في البحث داخل قائمة الكلمات |
| **أداء عالٍ** | مبني على طبقة الشبكات عالية الأداء NewLife.Net: إعادة استخدام الاتصالات، والقناة VIP، وضغط الرسائل، والتحكم في التزامن |

---

## التثبيت

```powershell
# مدير حزم NuGet
Install-Package NewLife.RocketMQ

# .NET CLI
dotnet add package NewLife.RocketMQ
```

```xml
<!-- PackageReference -->
<PackageReference Include="NewLife.RocketMQ" Version="3.0.*" />
```

---

## البدء السريع

### إرسال الرسائل

```csharp
using NewLife.RocketMQ;

var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    Group = "producer_group"
};
producer.Start();

// إرسال متزامن
var result = producer.Publish("Hello RocketMQ!");
Console.WriteLine($"Message ID: {result.MsgId}");

// إرسال غير متزامن
await producer.PublishAsync("async message");

// إرسال دفعي
await producer.PublishBatch(new[] { "message1", "message2", "message3" });
```

### استهلاك الرسائل

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
    return true; // إرجاع true يعني نجاح الاستهلاك
};

consumer.Start();
```

### الوصول عبر gRPC (Proxy 5.x)

```csharp
// المنتج: تعيين GrpcProxyAddress يوجّه حركة المرور عبر مسار gRPC في 5.x (يتطلب netstandard2.1+)
var producer = new Producer
{
    Topic = "test_topic",
    Group = "producer_group",
    NameServerAddress = "127.0.0.1:9876",
    GrpcProxyAddress = "http://127.0.0.1:8081",
};
producer.Start();
await producer.PublishAsync("gRPC message");

// المستهلك: تُبلَّغ علاقات الاشتراك تلقائيًا عند بدء التشغيل (اتصال Telemetry طويل الأمد)؛ ويجري الاستقبال والإقرار عبر gRPC
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
// ... معالجة الأعمال ...
await consumer.AckMessageViaGrpcAsync("test_topic",
[
    .. messages.Select(m => new AckMessageEntry
    {
        ReceiptHandle = m.SystemProperties?.ReceiptHandle,
        MessageId = m.SystemProperties?.MessageId,
    }),
]);
```

> يمكن أيضًا للاستهلاك بالاستقصاء الطويل استخدام `GrpcPushConsumer` مباشرة (دلالات PUSH_CONSUMER مع إقرار تلقائي وإعادة محاولة مدمجين).
>
> **إعادة محاولة POP والرسائل الميتة في 5.x**: الرسائل التي يفشل استهلاكها يكتبها Broker إلى `%RETRY%{group}_{topic}`، ثم تُدمَج مرة أخرى في استجابة السحب الخاصة بالموضوع الأصلي وفق `popFromRetryProbability`؛ ولا يحتاج العميل إلا إلى استهلاك الموضوع الأصلي. وعندما يبلغ عدد مرّات التسليم `GrpcPushConsumer.MaxDeliveryAttempts` (الافتراضي 16)، تُنقَل الرسالة إلى `%DLQ%{group}`؛ وفي سيناريوهات الاستقبال اليدوي، استدعِ `Consumer.ForwardToDeadLetterQueueViaGrpcAsync` (`GetGrpcDlqTopic()` يعيد اسم موضوع الرسائل الميتة). الحد الأدنى لزمن الخفاء هو 10 ثوانٍ (يُضبط تلقائيًا داخل المكتبة)؛ والقيم الأصغر يرفضها Broker.
>
> يجب معالجة رسائل قائمة الرسائل الميتة في تمريرة واحدة: `dotnet run --file scripts/DlqTool.cs -- --group <consumer group> --to <original topic>` يعيد نشر الرسائل من قائمة الرسائل الميتة إلى الموضوع المحدد ويقرّ فقط بالرسائل التي نجح نشرها. سحب رسائل قائمة الرسائل الميتة يقدّم إزاحة الاستهلاك، والرسائل غير المُقرّة ينقلها Broker إلى `%RETRY%{group}_%DLQ%{group}`؛ لذلك لا يُدعم التصفّح للقراءة فقط لقائمة الرسائل الميتة.
>
> **الرسائل المرتّبة FIFO والرسائل المعاملاتية**: يجب أن يتوافق نوع الموضوع مع نوع الرسالة؛ أنشئه بـ `CreateTopic(topic, queueNum, 0, TopicMessageType.Fifo/Transaction)`. في إرسال FIFO، عيّن `Message.MessageGroup`؛ وفي جانب المستهلك، عيّن `GrpcPushConsumer.Orderly = true` للمعالجة التسلسلية والحفاظ على الترتيب. وللرسائل المعاملاتية، استخدم `PublishTransactionViaGrpcAsync` لإرسال الرسالة النصفية و`EndTransactionViaGrpcAsync` للتثبيت أو التراجع (وإذا لم تُثبَّت، يتراجع Broker عنها بعد انتهاء المهلة).

### الرسائل المؤجلة

```csharp
// 18 مستوى تأخير مُعدًّا مسبقًا
producer.PublishDelay("delayed message", DelayTimeLevels.s30);

// وضع gRPC يدعم تأخيرات اعتباطية (يتطلب netstandard2.1+)
producer.GrpcProxyAddress = "http://127.0.0.1:8081";
await producer.PublishDelayViaGrpcAsync("arbitrary delay", DateTime.Now.AddMinutes(30));
```

### الرسائل المعاملاتية

```csharp
var producer = new Producer
{
    Topic = "tx_topic",
    Group = "tx_group",
    NameServerAddress = "127.0.0.1:9876"
};

// رد نداء مراجعة المعاملة
producer.OnCheckTransaction = (msg, transactionId) =>
{
    var success = CheckLocalTransaction(transactionId);
    return success ? TransactionState.Commit : TransactionState.Rollback;
};
producer.Start();

// إرسال الرسالة النصفية -> تنفيذ المعاملة المحلية -> التثبيت/التراجع
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

### الرسائل المرتّبة

```csharp
// الرسائل ذات المفتاح نفسه تذهب إلى القائمة نفسها
var queue = producer.SelectQueue("order_123");
producer.Publish("ordered message 1", queue);
producer.Publish("ordered message 2", queue);

// تمكين الاستهلاك المرتّب في جانب المستهلك
consumer.OrderConsume = true;
```

### نمط الطلب والرد

```csharp
// المنتج يرسل طلبًا (متزامن/غير متزامن)
var response = producer.Request("request message", timeout: 5000);
var reply = await producer.RequestAsync("async request", timeout: 5000);

// المستهلك يردّ
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

### تحويل الرسائل بين البروتوكولين

```csharp
// تحويل رسالة Remoting إلى رسالة gRPC (يتطلب netstandard2.1+)
var grpcMsg = message.ToGrpcMessage();

// تحويل رسالة gRPC إلى رسالة Remoting موسّعة / رسالة أساسية
var ext = grpcMsg.ToMessageExt();   // MessageExt (يشمل جميع خصائص النظام)
var msg = grpcMsg.ToMessage();      // Message (الحقول الأساسية)
```

---

## ميزات المستهلك المتقدمة

### إعادة محاولة الاستهلاك وقائمة الرسائل الميتة

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    EnableRetry = true,           // تمكين إعادة محاولة الاستهلاك
    MaxReconsumeTimes = 3         // الحد الأقصى لمرّات إعادة المحاولة؛ الرسائل التي تتجاوزه تدخل قائمة الرسائل الميتة %DLQ%
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        try { ProcessMessage(msg); }
        catch { return false; } // إرجاع false يطلق إعادة المحاولة
    }
    return true;
};
```

### تصفية Tag / SQL92

```csharp
// تصفية Tag
consumer.Tags = "TagA || TagB";

// تصفية بتعبير SQL92
consumer.ExpressionType = "SQL92";
consumer.Subscription = "age > 18 AND city = 'Shanghai'";
```

### الاشتراك في مواضيع متعددة

```csharp
consumer.Topics = "topic1;topic2;topic3";
```

### وضع استهلاك Pop

```csharp
// استهلاك Pop (إقرار يدوي)
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
        await consumer.ChangeInvisibleTimeAsync(msg, 30000); // تمديد زمن المعالجة
    }
}
```

### خفض معدّل الاستهلاك / القناة VIP / ضغط الرسائل

```csharp
consumer.MaxConcurrentConsume = 10; // معالجة 10 رسائل كحد أقصى في الوقت نفسه

producer.VipChannelEnabled = true;  // تمكين القناة VIP (BrokerPort - 2)
producer.CompressOverBytes = 4096;  // ضغط أجسام الرسائل الأكبر من 4KB تلقائيًا بـ ZLIB
```

### جسر HTTP (MQ -> WebAPI)

لتوجيه الرسائل المستهلكة إلى WebAPI، أو لإتاحة WebAPI للنشر الخارجي للرسائل، يمكنك الجمع بين واجهات API الحالية؛ راجع [دليل جسر HTTP](Doc/HttpBridge_Guide.md).

---

## الوصول إلى مزوّدي السحابة

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
        InstanceId = "MQ_INST_xxx"  // اختياري؛ يُحلَّل تلقائيًا من العنوان
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

### مصادقة Apache RocketMQ ACL

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

## النشر بـ NativeAOT

لا يدعم NativeAOT/trimming إلا هدف `net10.0` (أهداف الأُطر الأقدم لا تعرف مفهوم trimming وسلوكها لا يتغيّر). تتضمّن الحزمة بالفعل ملحقات وصول للأعضاء مولّدة من المصدر وجذور بيانات وصفية، لذلك لا حاجة إلى إعداد إضافي عند استهلاكها:

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

السيناريوهات المتحقَّق منها (مقيسة بملفات تنفيذية أصلية): ترميز الإطارات وفك ترميزها، وقواميس خصائص ترويسة الطلب، وتسلسل JSON، وضغط الرسائل (دورة ZLIB وحدود فك الضغط)، وخصائص LMQ، وتوقيع ACL، ومحوّلات مزوّدي السحابة، والتحويل بين البروتوكولين، والإرسال/الاستقبال من الطرف إلى الطرف (العادي/الجسم الكائني/الدفعي/المؤجل/ضغط الرسائل الكبيرة، والاستهلاك، واستعلام الإزاحة، وقفل الترتيب، وPOP) إضافة إلى مسار gRPC 5.x (التوجيه، وإقرار الإرسال/الاستقبال، واستهلاك الدفع، وإعادة توجيه الرسائل الميتة، والاستهلاك المرتّب FIFO).

تغطية CI: تُنشَر وتُشغَّل أصليًا كل من win-x64 (بما في ذلك اختبار طرف إلى طرف كامل لـ RocketMQ محليًا)، وlinux-x64، وlinux-arm64، وwin-arm64، وosx-x64/osx-arm64، وlinux-musl-x64/arm64؛ ويعمل Loongson linux-loongarch64 عبر محاكاة QEMU في وضع المستخدم مع Loongson SDK (لا يوجد حاليًا دعم NativeAOT رسمي لتلك المنصة، لذلك يُستخدم اختبار دخان JIT هندسي بنمط AOT كمعيار).

**يجب تسجيل أجسام الكائنات من الأنواع المخصّصة**: عند استخدام نوع مخصّص (POCO) كجسم رسالة، يحذف trimmer خصائصه العامة افتراضيًا، فيُسلسَل جسم الكائن بصمت ككائن فارغ `{}`. سجّل كل نوع يُستخدم كجسم رسالة مرة واحدة عند بدء التطبيق:

```csharp
MqAot.RegisterBodyType<MyOrder>();
MqAot.RegisterBodyType<MyEvent>();
```

لا تحمل `MqAot.RegisterBodyType<T>()` دلالات الحفاظ على trimming إلا على net10.0؛ وهي بلا أثر على الأهداف الأخرى، لذا يمكن للمشاريع متعددة الأهداف مشاركة الشيفرة نفسها عند بدء التشغيل. ويمكنك أيضًا تعريف جذور البيانات الوصفية بنفس النمط الذي تستخدمه المكتبة (`[DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MyOrder))]` + `[ModuleInitializer]`).

---

## نظرة عامة على البنية

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

راجع [مستند المتطلبات](Doc/需求文档.md)، و[قائمة الميزات](Doc/功能清单.md)، و[تصميم البنية](Doc/架构设计.md)، و[تحليل المنافسين](Doc/竞品分析.md)، و[دليل جسر HTTP](Doc/HttpBridge_Guide.md)، و[التوطين](Doc/多语言本地化.md).

---

## نظرة عامة على الميزات

### المنتج

| الميزة | الحالة | الوصف |
|---------|:------:|-------------|
| إرسال متزامن/غير متزامن/أحادي الاتجاه | ✅ | Publish / PublishAsync / PublishOneway |
| إرسال دفعي للرسائل | ✅ | PublishBatch، دمج عدة رسائل في طلب واحد |
| الرسائل المؤجلة | ✅ | 18 مستوى مُعدًّا مسبقًا + تأخيرات اعتباطية عبر gRPC |
| الرسائل المعاملاتية | ✅ | رسالة نصفية + تثبيت/تراجع + رد نداء المراجعة |
| الرسائل المرتّبة | ✅ | الإرسال إلى MessageQueue محدّدة |
| الطلب والرد | ✅ | طلب ورد متزامن/غير متزامن |
| ضغط الرسائل | ✅ | ضغط ZLIB تلقائي عند تجاوز حد CompressOverBytes |
| تتبّع الرسائل | ✅ | AsyncTraceDispatcher + MessageTraceHook |

### المستهلك

| الميزة | الحالة | الوصف |
|---------|:------:|-------------|
| استهلاك السحب / جدولة الاستهلاك | ✅ | سحب بالاستقصاء الطويل مع إسناد تلقائي للقوائم |
| استهلاك عنقودي/بثّي | ✅ | إسناد متوازن عبر Rebalance / حفظ الإزاحة محليًا |
| تصفية Tag / SQL92 | ✅ | تصفية بالتعبيرات |
| الاشتراك في مواضيع متعددة | ✅ | خاصية Topics؛ وRebalance لكل موضوع |
| إعادة محاولة الاستهلاك + قائمة الرسائل الميتة | ✅ | EnableRetry + MaxReconsumeTimes |
| الاستهلاك المرتّب | ✅ | قفل القوائم (OrderConsume) |
| استهلاك Pop | ✅ | Pop/Ack/BatchAck/ChangeInvisibleTime |
| خفض معدّل الاستهلاك | ✅ | تحكم بإشارة MaxConcurrentConsume |

### الإدارة والعمليات

| الميزة | الحالة | الوصف |
|---------|:------:|-------------|
| CRUD للمواضيع/مجموعات المستهلكين | ✅ | إنشاء/تحديث/حذف |
| استعلام الرسائل | ✅ | بالمعرّف / بالمفتاح |
| إحصاءات الاستهلاك / معلومات العنقود | ✅ | GetConsumeStats / GetClusterInfo |
| إدارة الإزاحة وإعادة تعيينها | ✅ | استعلام/تحديث/إعادة تعيين |

### البروتوكولات والتوافق

| إصدار الخادم | Remoting | gRPC | الوصف |
|----------------|:--------:|:----:|-------------|
| RocketMQ 4.0 ~ 4.9 | ✅ | — | متوافق تمامًا |
| RocketMQ 5.x (Broker) | ✅ | — | Remoting متوافق مع الإصدارات الأقدم |
| RocketMQ 5.x (Proxy) | — | ✅ | يُفعَّل عبر GrpcProxyAddress |
| Alibaba Cloud 4.x | ✅ | — | محوّل AliyunProvider |
| Huawei Cloud DMS | ✅ | — | محوّل HuaweiProvider |
| Tencent Cloud TDMQ | ✅ | — | محوّل TencentProvider |

---

## تغطية الاختبار

تغطي مجموعة اختبارات xUnit الوظائف الأساسية والميزات المتقدمة والتوافق مع البروتوكولات ومحوّلات مزوّدي السحابة واختبارات التكامل من الطرف إلى الطرف (تُتخطّى حالات تكامل gRPC تلقائيًا عند عدم إعداد Proxy)؛ كما تتحقّق من دلالات مصادقة Apache ACL 2.0 على Remoting وgRPC معًا (الطلبات بلا بيانات اعتماد، أو من مستخدمين غير إداريين، أو بمفاتيح خاطئة تُرفَض، بينما يستطيع الإداريون الإرسال والاستقبال بصورة طبيعية). تُشغَّل اختبارات الوحدة بالتوازي حسب صنف الاختبار، ويكتمل `dotnet test XUnitTestRocketMQ/XUnitTest.csproj --filter "Category!=Integration"` في نحو 4 ثوانٍ؛ وتُوضَع حالات التكامل التي تعتمد على Broker حقيقي في مجموعة RocketMQ وتُشغَّل تسلسليًا لتجنّب تداخل المواضيع ومجموعات المستهلكين المشتركة؛ عيّن `ROCKETMQ_TEST_VERBOSE=1` لاستعادة سجلات الإطار عند استكشاف الأخطاء. يعمل CI كمصفوفة RID: تغطي اختبارات الوحدة واختبارات دخان AOT كلاً من win-x64 / linux-x64 / linux-arm64 / Loongson LA64 (محاكاة QEMU، إلزامية)، بينما win-arm64 ومعماريتا osx ومعماريتا linux-musl في فترة المراقبة؛ وجميعها تدعم التشغيل اليدوي.

يتضمّن أيضًا: اختبارات دخان NativeAOT (`AotSmoke`، تشمل ضغط الرسائل وLMQ وتوقيع ACL ومسار gRPC وإعادة توجيه الرسائل الميتة والاستهلاك المرتّب FIFO)، واختبارات دخان AOT لاستهلاك حزمة NuGet (`AotPackageSmoke`، تستهلك النواتج المحزومة عبر PackageReference وتغطي أصول الحزمة والتسلسل المولَّد من المصدر والضغط والإرسال/الاستقبال من الطرف إلى الطرف)، وجمع خط الأساس للنشر (`scripts/AotBaseline.cs`، يبلّغ عن الحجم والبصمة الزمنية عند بدء التشغيل والإنتاجية مع فحوص الحدود ويُكتب إلى ملخّص الخط في CI)، ومقارنات أداء AOT/JIT (مشروع `Benchmark` المبني على BenchmarkDotNet مع إحصاءات التخصيص؛ ويستخدم جانب AOT أخذ عينات متعددة الجولات عبر `AotSmoke --bench`)، وأداة عمليات قائمة الرسائل الميتة (`scripts/DlqTool.cs`، إعادة نشر رسائل `%DLQ%{group}` إلى موضوع محدّد)، ونص قبول استقرار gRPC (`scripts/GrpcStability.cs`، يُشغَّل ليليًا في CI مع إعادة تشغيل Proxy واحدة محقونة).

### اختبارات التكامل المحلية (Windows)

```powershell
# تثبيت وتشغيل NameServer + Broker + Proxy بخطوة واحدة (تُفرَّغ قاعدة التخزين عند كل تشغيل)
dotnet run --file scripts/RocketMqSetup.cs

$env:ROCKETMQ_NAMESERVER = "127.0.0.1:9876"
dotnet test XUnitTestRocketMQ\XUnitTest.csproj -c Release

# الإيقاف / الحالة
dotnet run --file scripts/RocketMqSetup.cs -- --stop
dotnet run --file scripts/RocketMqSetup.cs -- --status
```

يستخدم التشغيل المحلي وضع الموارد المنخفضة افتراضيًا: تُخفَّض سجلات الآلات الافتراضية الثلاث إلى WARN (ولا تُكتب بعد الآن إلى `%USERPROFILE%\logs\rocketmqlogs`)، وتصبح فترة التفريغ غير المتزامن 5 ثوانٍ، وحجم commitlog هو 64MB، وتُشدَّد مجمّعات الخيوط، وتُفرَّغ قاعدة التخزين عند كل تشغيل ليكون Broker دائمًا في حالة نظيفة. عند استكشاف الأخطاء، استخدم `--log-level INFO` (أو `$env:ROCKETMQ_TEST_LOG_LEVEL="INFO"`) لاستعادة السجلات التفصيلية (يزداد حجم السجل بشكل ملحوظ)؛ وعند عدم الحاجة إلى gRPC، استخدم `--no-proxy` لتشغيل آلة افتراضية أقل؛ ولتوجيه قاعدة التخزين إلى قرص RAM، استخدم `--store-dir R:\RocketMQ\store`.

---

## المساهمة

نرحّب بـ Issues وPull Requests!

1. انسخ هذا المستودع (Fork)
2. أنشئ فرعًا للميزة (`git checkout -b feature/AmazingFeature`)
3. ثبّت تغييراتك (`git commit -m 'Add some AmazingFeature'`)
4. ادفع إلى الفرع (`git push origin feature/AmazingFeature`)
5. افتح Pull Request

---

## الترخيص

يُصدَر هذا المشروع بموجب [رخصة MIT](LICENSE).

---


## خريطة المنظومة

NewLife منظومة متعدّدة الطبقات من مشاريع مفتوحة المصدر تُصدَر بإصدارات مستقلة. الخريطة أدناه مشتركة حرفيًا بين جميع مستودعات NewLife؛ وكل مشروع يرتبط بمستودعه وبحزمته الرئيسية على NuGet.

| المشروع | الوظيفة | التنزيلات |
| --- | --- | --- |
| **الأساس** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | التسجيل والتهيئة والتخزين المؤقت والشبكات والتسلسل والجدولة وتتبّع أداء APM. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | وسيط بيانات: جداول واحدة بمستوى عشرات المليارات من الصفوف، تقسيم تلقائي للجداول، فصل القراءة عن الكتابة، وأكثر من 10 قواعد بيانات. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | مكتبة شبكات عالية الأداء: 1.77 مليون pps / 14.5 Gbps على جهاز واحد (TCP 1KB، 2026-10)، و4 ملايين اتصال TCP طويل الأمد. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | إطار اتصال بروتوكولي وRPC لتطبيقات العميل/الخادم وأجهزة إنترنت الأشياء. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | يثبّت تطبيقات .NET كخدمات حارسة لنظام التشغيل: خدمات Windows وsystemd في Linux. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | قوالب مشاريع `dotnet new` للطرفية وweb وAPI والخدمات والبيانات وإنترنت الأشياء. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **البيانات والمراسلة** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | عميل Redis بزمن استجابة بالميكروثانية، وملايين العمليات في الثانية، وقوائم رسائل مدمجة. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | عميل RocketMQ مُدار بالكامل لـ Apache RocketMQ وخدمة رسائل Alibaba Cloud. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | عميل وخادم MQTT، بما في ذلك الاتصال بـ Alibaba Cloud IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | قاعدة بيانات هجينة بلغة C#: مدمجة أو شبكية، وعلائقية، وسلاسل زمنية، وقائمة رسائل، وKV في واحد. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | برنامج تشغيل MySql وطبقة مساعدة مبنية على مكدّس بيانات NewLife. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web والذكاء الاصطناعي والأنظمة الموزّعة** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | إطار منصة إدارة سريعة: الصلاحيات، والدخول الموحّد SSO، وخادم OAuth، وتوليد واجهات CRUD. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | منصة خدمات موزّعة: إدارة العقد، وAPM، ومركز الإعدادات، ومركز التسجيل، ومركز النشر. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | منصة موزّعة للمهام وجدولة البيانات الفورية، مع التقسيم وإعادة المحاولة وإعادة التعبئة. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | عميل موحّد لـ 46 مزوّد LLM بالإضافة إلى ChatAI، تطبيق ويب جاهز للمحادثة. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **الصناعة وإنترنت الأشياء** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | مكتبة معايير إنترنت الأشياء التي تعرّف عقود البروتوكولات وتجريدات الأجهزة. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | تنفيذ Modbus TCP/RTU/ASCII فوق NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | بروتوكول Siemens PLC فوق NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | بروتوكول Omron PLC فوق NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | بروتوكول Mitsubishi Melsec PLC فوق NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | عميل OPC للوصول إلى البيانات الصناعية. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | مكدّس بروتوكول BACnet لأتمتة المباني. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | منصة إنترنت أشياء مفتوحة المصدر وخفيفة، مع إدارة البوابات والأجهزة. | — |
| **تليماتيكا المركبات** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | بروتوكول تحديد مواقع المركبات JT/T 808: أكثر من 70 جسم رسالة، 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | بروتوكول JT/T 809 بين المنصات لجهات إدارة النقل. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | فيديو داخل المركبة JT/T 1078: تحليل RTP وإعادة التجميع إلى FLV/fMP4 والبث المباشر. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **مكوّنات المجال** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | حزمة واحدة لـ Excel وWord وPPT وPDF وأكثر من 15 صيغة مستندات. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | صوت عبر المنصات: مرمّزات، وإدخال/إخراج الأجهزة، وDSP، وحاويات، وبث. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | مرمّزات الفيديو وصيغ الحاويات ومعالجة الإطارات والبث. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | واجهة موحّدة فوق Baidu وAMap وTencent وTianditu مع الترميز الجغرافي وتحويل الإحداثيات. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | تحديد الموقع الجغرافي لعناوين IPv4 بقاعدة بيانات غير متصلة مُعيّنة في الذاكرة، وواجهة API عبر الإنترنت كبديل. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **الأدوات والمنتجات** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | صندوق أدوات المطوّر: الشبكات والمنفذ التسلسلي والتشفير والتعبيرات النمطية وModbus وMQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | نظام استضافة ذاتية لإدارة الشيفرة المصدرية Git. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | يثبّت بيئة تشغيل .NET المطابقة تلقائيًا عند التشغيل من مضيف أقدم. | — |

> تعرض الشارات الحزمة الرئيسية لكل مشروع؛ أما حزم السمات والإضافات والحزم التابعة فمذكورة في صفحاتها على NuGet. الحزم التي تتجاوز 30 حزمة وغير المذكورة هنا (FTP وLoRa وإضافات Office وبرامج تشغيل البروتوكولات وغيرها) متاحة في [منظمة NewLifeX](https://github.com/NewLifeX) وعلى [newlifex.com](https://newlifex.com).

---

## الفريق

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

تأسّست NewLife في عام 2002، وأمضت أكثر من عقدين في منظومة .NET وإنترنت الأشياء، وتدير ما يقارب 100 مشروع مفتوح المصدر (34 منها مشاريع رئيسية)، وتقدّم حلولًا متكاملة للبرمجيات والأجهزة لقطاعات الكهرباء والخدمات اللوجستية والرعاية الصحية والتحكّم الصناعي والإنترنت. ونلتزم بمبدأ "لا أسبقية في التعلّم — الإتقان يكسب الاحترام".

نُزّلت المكوّنات مفتوحة المصدر <!--nuget-total:start-->6.48M<!--nuget-total:end--> مرة من NuGet (على مستوى المنظومة بأكملها، حتى 2026-10).

الموقع: https://newlifex.com · المنظمة مفتوحة المصدر: https://github.com/NewLifeX

<details>
<summary>حساب WeChat الرسمي: 智能大石头 (امسح للاشتراك)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
