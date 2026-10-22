# NewLife.RocketMQ - Корпоративный полностью управляемый клиент RocketMQ

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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> Перевод может отставать от [английской версии](README.md).

**Корпоративный полностью управляемый клиент RocketMQ** с поддержкой `.NET Framework 4.5+` / `.NET Standard 2.0+` / `.NET Core` / `.NET 5+`.  
**Полностью реализован на C# без внешних зависимостей (не требуются сторонние библиотеки Java, gRPC или Protobuf).**

---

## Обзор продукта

NewLife.RocketMQ — **корпоративный полностью управляемый клиент RocketMQ**, разработанный командой NewLife для экосистемы .NET. Поддерживает как **протокол Remoting (Broker 4.x/5.x)**, так и **протокол gRPC Proxy (Proxy 5.x)**, охватывает все ключевые возможности производителя и потребителя, а также корпоративные функции, и предоставляет единые адаптеры для Alibaba Cloud, Huawei Cloud, Tencent Cloud и системы аутентификации Apache ACL.

**Ключевые преимущества**:

| Возможность | Описание |
|---------|-------------|
| **Поддержка двух протоколов** | Remoting (зрелый и стабильный на 4.x) + gRPC (ориентирован на 5.x), с автоматической маршрутизацией |
| **Ноль внешних зависимостей** | Встроенный кодек полей Protobuf (написан вручную на SpanWriter/SpanReader из NewLife.Core), не требуется среда выполнения Java или gRPC |
| **Поддержка нескольких облаков** | Единый интерфейс `ICloudProvider` со встроенными адаптерами для Alibaba Cloud, Huawei Cloud, Tencent Cloud и Apache ACL |
| **Готовность к эксплуатации** | Полный набор корпоративных функций: повторное потребление, очередь недоставленных сообщений, проверка транзакций, упорядоченное потребление, Pop-потребление и многое другое |
| **Самый широкий охват платформ** | От .NET Framework 4.5+ до .NET 10; функции gRPC доступны начиная с .NET Standard 2.1+ |
| **Дружелюбность к AOT** | Целевая платформа net10.0 поддерживает NativeAOT/тримминг: кодирование и декодирование кадров, сериализация JSON и сжатие сообщений не зависят от рефлексии времени выполнения. В репозитории есть AOT smoke-тест и AOT smoke-тест потребления NuGet-пакета; CI проверяет публикацию и запуск AOT на матрице основных RID (win/linux/osx, musl и Loongson LA64) |
| **Локализация во время выполнения** | Логи, исключения разбора протокола, исключения публичного API и описания конфигурации используют английский текст как исходный, со встроенными словарями для 31 языка (132 записи); перевод выполняется автоматически в соответствии с `Localization.Culture` с откатом на английский; логи с префиксом переводятся до формирования префикса, и префикс не участвует в поиске по словарю |
| **Высокая производительность** | Построен на высокопроизводительном сетевом слое NewLife.Net: повторное использование соединений, VIP-канал, сжатие сообщений и управление конкурентностью |

---

## Установка

```powershell
# Менеджер пакетов NuGet
Install-Package NewLife.RocketMQ

# .NET CLI
dotnet add package NewLife.RocketMQ
```

```xml
<!-- PackageReference -->
<PackageReference Include="NewLife.RocketMQ" Version="3.0.*" />
```

## Быстрый старт

### Отправка сообщений

```csharp
using NewLife.RocketMQ;

var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    Group = "producer_group"
};
producer.Start();

// Синхронная отправка
var result = producer.Publish("Hello RocketMQ!");
Console.WriteLine($"Message ID: {result.MsgId}");

// Асинхронная отправка
await producer.PublishAsync("async message");

// Пакетная отправка
await producer.PublishBatch(new[] { "message1", "message2", "message3" });
```

### Получение сообщений

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
    return true; // true означает успешное потребление
};

consumer.Start();
```

### Доступ по gRPC (Proxy 5.x)

```csharp
// Производитель: установка GrpcProxyAddress направляет трафик по пути gRPC 5.x (требуется netstandard2.1+)
var producer = new Producer
{
    Topic = "test_topic",
    Group = "producer_group",
    NameServerAddress = "127.0.0.1:9876",
    GrpcProxyAddress = "http://127.0.0.1:8081",
};
producer.Start();
await producer.PublishAsync("gRPC message");

// Потребитель: подписки автоматически сообщаются при запуске (постоянное соединение Telemetry); получение и подтверждение идут по gRPC
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
// ... обработка бизнес-логики ...
await consumer.AckMessageViaGrpcAsync("test_topic",
[
    .. messages.Select(m => new AckMessageEntry
    {
        ReceiptHandle = m.SystemProperties?.ReceiptHandle,
        MessageId = m.SystemProperties?.MessageId,
    }),
]);
```

> Для длинного опроса можно также напрямую использовать `GrpcPushConsumer` (семантика PUSH_CONSUMER со встроенными автоматическими Ack и повторами).
>
> **Повторы и недоставленные сообщения POP в 5.x**: сообщения, обработка которых завершилась ошибкой, записываются Broker-ом в `%RETRY%{group}_{topic}` и в соответствии с `popFromRetryProbability` возвращаются в ответ на выборку исходной темы; клиенту достаточно потреблять исходную тему. Когда число доставок достигает `GrpcPushConsumer.MaxDeliveryAttempts` (по умолчанию 16), сообщение перемещается в `%DLQ%{group}`; в сценариях ручного получения вызывайте `Consumer.ForwardToDeadLetterQueueViaGrpcAsync` (`GetGrpcDlqTopic()` возвращает имя темы недоставленных сообщений). Нижняя граница времени невидимости — 10 секунд (внутри библиотеки значение приводится автоматически); меньшие значения отклоняются Broker-ом.
>
> Сообщения из очереди недоставленных должны обрабатываться за один проход: `dotnet run --file scripts/DlqTool.cs -- --group <consumer group> --to <original topic>` повторно публикует сообщения из очереди недоставленных в указанную тему и подтверждает только успешно переопубликованные. Выборка сообщений из очереди недоставленных сдвигает смещение потребления, а неподтверждённые сообщения переносятся Broker-ом в `%RETRY%{group}_%DLQ%{group}`; поэтому просмотр очереди недоставленных в режиме только чтения не поддерживается.
>
> **Упорядоченные FIFO-сообщения и транзакционные сообщения**: тип темы должен соответствовать типу сообщений; создавайте её через `CreateTopic(topic, queueNum, 0, TopicMessageType.Fifo/Transaction)`. Для FIFO-отправки задайте `Message.MessageGroup`; на стороне потребителя установите `GrpcPushConsumer.Orderly = true`, чтобы обрабатывать последовательно и сохранять порядок. Для транзакционных сообщений используйте `PublishTransactionViaGrpcAsync` для отправки половинного сообщения и `EndTransactionViaGrpcAsync` для фиксации или отката (если фиксации не было, Broker откатит сообщение по тайм-ауту).

### Отложенные сообщения

```csharp
// 18 предустановленных уровней задержки
producer.PublishDelay("delayed message", DelayTimeLevels.s30);

// Режим gRPC поддерживает произвольные задержки (требуется netstandard2.1+)
producer.GrpcProxyAddress = "http://127.0.0.1:8081";
await producer.PublishDelayViaGrpcAsync("arbitrary delay", DateTime.Now.AddMinutes(30));
```

### Транзакционные сообщения

```csharp
var producer = new Producer
{
    Topic = "tx_topic",
    Group = "tx_group",
    NameServerAddress = "127.0.0.1:9876"
};

// Обратный вызов проверки транзакции
producer.OnCheckTransaction = (msg, transactionId) =>
{
    var success = CheckLocalTransaction(transactionId);
    return success ? TransactionState.Commit : TransactionState.Rollback;
};
producer.Start();

// Отправка половинного сообщения -> выполнение локальной транзакции -> фиксация/откат
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

### Упорядоченные сообщения

```csharp
// Сообщения с одинаковым ключом попадают в одну очередь
var queue = producer.SelectQueue("order_123");
producer.Publish("ordered message 1", queue);
producer.Publish("ordered message 2", queue);

// Включение упорядоченного потребления на стороне потребителя
consumer.OrderConsume = true;
```

### Шаблон «запрос-ответ»

```csharp
// Производитель отправляет запрос (синхронно/асинхронно)
var response = producer.Request("request message", timeout: 5000);
var reply = await producer.RequestAsync("async request", timeout: 5000);

// Потребитель отвечает
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

### Преобразование сообщений между протоколами

```csharp
// Преобразование сообщения Remoting в сообщение gRPC (требуется netstandard2.1+)
var grpcMsg = message.ToGrpcMessage();

// Преобразование сообщения gRPC в расширенное сообщение Remoting / базовое сообщение
var ext = grpcMsg.ToMessageExt();   // MessageExt (включает все системные свойства)
var msg = grpcMsg.ToMessage();      // Message (базовые поля)
```

---

## Расширенные возможности потребителя

### Повторное потребление и очередь недоставленных сообщений

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    EnableRetry = true,           // Включить повторное потребление
    MaxReconsumeTimes = 3         // Максимальное число повторов; превысившие сообщения попадают в очередь недоставленных %DLQ%
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        try { ProcessMessage(msg); }
        catch { return false; } // false запускает повтор
    }
    return true;
};
```

### Фильтрация по Tag / SQL92

```csharp
// Фильтрация по Tag
consumer.Tags = "TagA || TagB";

// Фильтрация по выражению SQL92
consumer.ExpressionType = "SQL92";
consumer.Subscription = "age > 18 AND city = 'Shanghai'";
```

### Подписка на несколько тем

```csharp
consumer.Topics = "topic1;topic2;topic3";
```

### Режим Pop-потребления

```csharp
// Pop-потребление (ручное подтверждение)
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
        await consumer.ChangeInvisibleTimeAsync(msg, 30000); // Продлить время обработки
    }
}
```

### Ограничение потребления / VIP-канал / сжатие сообщений

```csharp
consumer.MaxConcurrentConsume = 10; // Обрабатывать не более 10 сообщений одновременно

producer.VipChannelEnabled = true;  // Включить VIP-канал (BrokerPort - 2)
producer.CompressOverBytes = 4096;  // Автоматически сжимать тела сообщений больше 4 КБ через ZLIB
```

### HTTP-мост (MQ -> WebAPI)

Чтобы пересылать полученные сообщения в WebAPI или предоставить WebAPI для внешней публикации сообщений, можно скомбинировать существующие API; см. [руководство по HTTP-мосту](Doc/HttpBridge_Guide.md).

---

## Доступ к облачным провайдерам

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
        InstanceId = "MQ_INST_xxx"  // Необязательно; разбирается автоматически из адреса
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

### Аутентификация Apache RocketMQ ACL

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

## Публикация NativeAOT

Только целевая платформа `net10.0` поддерживает NativeAOT/тримминг (у более старых целевых платформ нет понятия тримминга, их поведение не меняется). Пакет уже содержит сгенерированные исходным кодом аксессоры членов и корневые метаданные, поэтому при использовании дополнительная конфигурация не нужна:

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

Проверенные сценарии (измерено на нативных исполняемых файлах): кодирование и декодирование кадров, словари свойств заголовков запросов, сериализация JSON, сжатие сообщений (цикл ZLIB и ограничения распаковки), свойства LMQ, подпись ACL, адаптеры облачных провайдеров, преобразование между протоколами и сквозная отправка/получение (обычное тело/объектное тело/пакет/задержка/сжатие больших сообщений, потребление, запрос смещений, упорядоченная блокировка, POP), а также путь gRPC 5.x (маршрутизация, получение и подтверждение, push-потребление, пересылка в очередь недоставленных, упорядоченное FIFO-потребление).

Покрытие CI: win-x64 (включая полную локальную сквозную проверку RocketMQ), linux-x64, linux-arm64, win-arm64, osx-x64/osx-arm64, linux-musl-x64/arm64 — каждая платформа нативно публикуется и запускается; Loongson linux-loongarch64 запускается через эмуляцию пользовательского режима QEMU вместе с SDK Loongson (официальной поддержки NativeAOT для этой платформы сейчас нет, поэтому критерием служит AOT-подобный инженерный JIT smoke-тест).

**Тела пользовательских типов-объектов обязательно нужно регистрировать**: когда в качестве тела сообщения используется пользовательский тип (POCO), триммер по умолчанию удаляет его публичные свойства, и объектное тело молча сериализуется как пустой объект `{}`. Зарегистрируйте каждый тип, используемый как тело сообщения, один раз при запуске приложения:

```csharp
MqAot.RegisterBodyType<MyOrder>();
MqAot.RegisterBodyType<MyEvent>();
```

`MqAot.RegisterBodyType<T>()` имеет семантику сохранения при тримминге только на net10.0; на других целях это пустая операция, поэтому мультицелевые проекты могут использовать один и тот же код запуска. Вы также можете объявить собственные корневые метаданные по тому же образцу, что и библиотека (`[DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MyOrder))]` + `[ModuleInitializer]`).

---

## Обзор архитектуры

```
MqBase (базовый бизнес-класс)
├── Producer
└── Consumer

Слой связи
├── Протокол Remoting (Broker 4.x/5.x)
│   ├── ClusterClient (постоянное TCP-соединение, переиспользование Opaque)
│   ├── NameClient (обнаружение маршрутов, опрос каждые 30 с)
│   └── BrokerClient (heartbeat/unregister)
│
└── Протокол gRPC (Proxy 5.x, netstandard2.1+)
    ├── GrpcClient (HTTP/2, Unary + Server Streaming + постоянное соединение только на запись)
    ├── GrpcMessagingService (11 RPC-методов)
    ├── GrpcTelemetrySession (постоянное соединение отчётов о ресурсах: периодическая отправка + переподключение при разрыве)
    ├── MessageConverter (преобразование сообщений Remoting ↔ gRPC)
    └── Кодек полей (Protobuf, написанный вручную на SpanWriter/SpanReader)

Слой адаптеров облачных провайдеров
├── AliyunProvider  (Alibaba Cloud: маршрутизация по InstanceId + HTTP NameServer)
├── HuaweiProvider  (Huawei Cloud: SSL/TLS + маршрутизация по InstanceId)
├── TencentProvider (Tencent Cloud: маршрутизация по префиксу Namespace)
└── AclProvider     (Apache ACL: подпись HMAC-SHA1)
```

См. [документ требований](Doc/需求文档.md), [список функций](Doc/功能清单.md), [архитектурный дизайн](Doc/架构设计.md), [анализ конкурентов](Doc/竞品分析.md), [руководство по HTTP-мосту](Doc/HttpBridge_Guide.md) и [локализацию](Doc/多语言本地化.md).

---

## Обзор возможностей

### Производитель

| Возможность | Статус | Описание |
|---------|:------:|-------------|
| Синхронная/асинхронная/односторонняя отправка | ✅ | Publish / PublishAsync / PublishOneway |
| Пакетная отправка сообщений | ✅ | PublishBatch, объединение нескольких сообщений в один запрос |
| Отложенные сообщения | ✅ | 18 предустановленных уровней + произвольные задержки по gRPC |
| Транзакционные сообщения | ✅ | Половинное сообщение + фиксация/откат + обратный вызов проверки |
| Упорядоченные сообщения | ✅ | Отправка в указанную MessageQueue |
| Запрос-ответ | ✅ | Синхронный/асинхронный запрос-ответ |
| Сжатие сообщений | ✅ | Автоматический ZLIB при превышении порога CompressOverBytes |
| Трассировка сообщений | ✅ | AsyncTraceDispatcher + MessageTraceHook |

### Потребитель

| Возможность | Статус | Описание |
|---------|:------:|-------------|
| Вытягивающее потребление / планирование потребления | ✅ | Длинный опрос с автоматическим распределением очередей |
| Кластерное/широковещательное потребление | ✅ | Перебалансировка с равномерным распределением / локальное сохранение смещений |
| Фильтрация по Tag / SQL92 | ✅ | Фильтрация по выражениям |
| Подписка на несколько тем | ✅ | Свойство Topics; перебалансировка по каждой теме |
| Повторное потребление + очередь недоставленных | ✅ | EnableRetry + MaxReconsumeTimes |
| Упорядоченное потребление | ✅ | Блокировка очередей (OrderConsume) |
| Pop-потребление | ✅ | Pop/Ack/BatchAck/ChangeInvisibleTime |
| Ограничение потребления | ✅ | Управление семафором MaxConcurrentConsume |

### Управление и эксплуатация

| Возможность | Статус | Описание |
|---------|:------:|-------------|
| CRUD тем и групп потребителей | ✅ | Создание/изменение/удаление |
| Запрос сообщений | ✅ | По ID / по Key |
| Статистика потребления / информация о кластере | ✅ | GetConsumeStats / GetClusterInfo |
| Управление и сброс смещений | ✅ | Запрос/обновление/сброс |

### Протоколы и совместимость

| Версия сервера | Remoting | gRPC | Описание |
|----------------|:--------:|:----:|-------------|
| RocketMQ 4.0 ~ 4.9 | ✅ | — | Полная совместимость |
| RocketMQ 5.x (Broker) | ✅ | — | Remoting обратно совместим |
| RocketMQ 5.x (Proxy) | — | ✅ | Включается через GrpcProxyAddress |
| Alibaba Cloud 4.x | ✅ | — | Адаптер AliyunProvider |
| Huawei Cloud DMS | ✅ | — | Адаптер HuaweiProvider |
| Tencent Cloud TDMQ | ✅ | — | Адаптер TencentProvider |

---

## Тестовое покрытие

Набор xUnit-тестов охватывает ключевую функциональность, расширенные возможности, совместимость протоколов, адаптеры облачных провайдеров и сквозные интеграционные тесты (интеграционные случаи gRPC автоматически пропускаются, если Proxy не настроен); он также проверяет семантику аутентификации Apache ACL 2.0 как на Remoting, так и на gRPC (запросы без учётных данных, от пользователей без прав администратора или с неверными ключами отклоняются, а администраторы могут нормально отправлять и получать сообщения). Модульные тесты выполняются параллельно по классам, а `dotnet test XUnitTestRocketMQ/XUnitTest.csproj --filter "Category!=Integration"` завершается примерно за 4 секунды; интеграционные случаи, зависящие от реального Broker, помещены в коллекцию RocketMQ и выполняются последовательно, чтобы избежать взаимных помех из-за общих тем и групп потребителей; для восстановления журналов фреймворка при диагностике задайте `ROCKETMQ_TEST_VERBOSE=1`. CI работает как матрица RID: модульные тесты и AOT smoke-тесты охватывают win-x64 / linux-x64 / linux-arm64 / Loongson LA64 (эмуляция QEMU, обязательна), а win-arm64, обе архитектуры osx и обе архитектуры linux-musl находятся в периоде наблюдения; все они поддерживают ручной запуск.

Также включены: smoke-тесты NativeAOT (`AotSmoke`, включая сжатие сообщений, LMQ, подпись ACL, путь gRPC, пересылку в очередь недоставленных и упорядоченное FIFO-потребление), AOT smoke-тесты потребления NuGet-пакета (`AotPackageSmoke`, потребление упакованных артефактов через PackageReference, охватывая ресурсы пакета, сгенерированную сериализацию, сжатие и сквозную отправку/получение), сбор базовых показателей публикации (`scripts/AotBaseline.cs`, отчёт о размере, накладных расходах на запуск и пропускной способности с проверкой порогов и записью в сводку конвейера в CI), бенчмарки сравнения производительности AOT/JIT (проект `Benchmark` на основе BenchmarkDotNet, включая статистику аллокаций; сторона AOT использует многократную выборку через `AotSmoke --bench`), инструмент работы с очередью недоставленных (`scripts/DlqTool.cs`, повторная публикация сообщений `%DLQ%{group}` в указанную тему) и скрипт приёмки стабильности gRPC (`scripts/GrpcStability.cs`, запускается ночью в CI с одной инъекцией перезапуска Proxy).

### Локальные интеграционные тесты (Windows)

```powershell
# Установка и запуск NameServer + Broker + Proxy одной командой (хранилище очищается при каждом запуске)
dotnet run --file scripts/RocketMqSetup.cs

$env:ROCKETMQ_NAMESERVER = "127.0.0.1:9876"
dotnet test XUnitTestRocketMQ\XUnitTest.csproj -c Release

# Остановка / статус
dotnet run --file scripts/RocketMqSetup.cs -- --stop
dotnet run --file scripts/RocketMqSetup.cs -- --status
```

Локальный запуск по умолчанию использует режим малых ресурсов: журналы трёх JVM сокращаются до WARN (больше не пишутся в `%USERPROFILE%\logs\rocketmqlogs`), интервал асинхронного сброса — 5 секунд, commitlog — 64 МБ, пулы потоков ужаты, а хранилище очищается при каждом запуске, поэтому Broker всегда находится в свежем состоянии. При диагностике используйте `--log-level INFO` (или `$env:ROCKETMQ_TEST_LOG_LEVEL="INFO"`), чтобы вернуть подробные журналы (объём журналов заметно возрастает); если gRPC не нужен, используйте `--no-proxy`, чтобы запускать на одну JVM меньше; чтобы разместить хранилище на RAM-диске, используйте `--store-dir R:\RocketMQ\store`.

---

## Участие в разработке

Issues и Pull Requests приветствуются!

1. Форкните этот репозиторий
2. Создайте ветку функциональности (`git checkout -b feature/AmazingFeature`)
3. Зафиксируйте изменения (`git commit -m 'Add some AmazingFeature'`)
4. Отправьте изменения в ветку (`git push origin feature/AmazingFeature`)
5. Откройте Pull Request

---

## Лицензия

Проект распространяется по [лицензии MIT](LICENSE).

---

## Карта экосистемы

NewLife — это многоуровневая экосистема независимо версионируемых open-source проектов. Карта ниже дословно используется во всех репозиториях NewLife; каждый проект ссылается на свой репозиторий и свой основной пакет NuGet.

| Проект | Что делает | Загрузки |
| --- | --- | --- |
| **Базовый** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | Логирование, конфигурация, кэширование, сеть, сериализация, планирование и APM-трассировка. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | Промежуточное ПО данных: отдельные таблицы масштаба десятков миллиардов строк, автоматическое секционирование, разделение чтения и записи, более 10 баз данных. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | Высокопроизводительная сетевая библиотека: 1,77M pps / 14,5 Гбит/с на машину (TCP 1KB, 2026-10), 4M долгоживущих TCP-соединений. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | Протокольная связь и RPC-фреймворк для клиент-серверных приложений и IoT-устройств. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | Устанавливает .NET-приложения как демоны ОС: службы Windows и systemd в Linux. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | Шаблоны проектов `dotnet new` для консоли, web, API, службы, данных и IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **Данные и обмен сообщениями** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Клиент Redis с микросекундной задержкой, миллионами операций в секунду и встроенными очередями сообщений. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Полностью управляемый клиент RocketMQ для Apache RocketMQ и Alibaba Cloud Message Queue. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | Клиент и сервер MQTT, включая подключение к Alibaba Cloud IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | Гибридная база данных на C#: встроенная или сетевая, реляционная, временных рядов, очередь сообщений и KV в одном. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | Драйвер MySql и вспомогательный слой на основе стека данных NewLife. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web, AI и распределённые системы** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | Фреймворк быстрой разработки админ-платформ: права, SSO, сервер OAuth и CRUD-скаффолдинг. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | Распределённая сервисная платформа: управление узлами, APM, центр конфигурации, реестр и центр публикации. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | Распределённая платформа задач и планирования данных в реальном времени с секционированием, повторами и дозаполнением. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | Единый клиент для 46 поставщиков LLM плюс ChatAI — готовое веб-приложение для чата. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **Промышленность и IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | Стандартная библиотека IoT, определяющая контракты протоколов и абстракции устройств. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | Реализация Modbus TCP/RTU/ASCII поверх NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | Протокол Siemens PLC поверх NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | Протокол Omron PLC поверх NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | Протокол Mitsubishi Melsec PLC поверх NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | Клиент OPC для доступа к промышленным данным. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | Стек протокола автоматизации зданий BACnet. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | Лёгкая open-source платформа IoT с управлением шлюзами и устройствами. | — |
| **Телематика транспорта** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | Протокол позиционирования транспорта JT/T 808: более 70 типов сообщений, 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | Межплатформенный протокол JT/T 809 для органов управления транспортом. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | Автомобильное видео JT/T 1078: разбор RTP, ремультиплексирование в FLV/fMP4 и прямая трансляция. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **Прикладные компоненты** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Один пакет для Excel, Word, PPT, PDF и более 15 форматов документов. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | Кроссплатформенное аудио: кодеки, ввод-вывод устройств, DSP, контейнеры и потоковая передача. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | Видеокодеки, форматы контейнеров, обработка кадров и потоковая передача. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | Единый интерфейс к Baidu, AMap, Tencent и Tianditu с геокодированием и преобразованием координат. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | Геолокация IPv4 с офлайн-базой, отображаемой в память, и онлайн-API как резервом. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **Инструменты и продукты** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | Инструменты разработчика: сеть, последовательный порт, шифрование, регулярные выражения, Modbus и MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | Самостоятельно размещаемая система управления исходным кодом Git. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | Автоматически устанавливает подходящую среду выполнения .NET при запуске на старом хосте. | — |

> Бейджи показывают основной пакет каждого проекта; пакеты тем, расширений и сателлитов перечислены на соответствующих страницах NuGet. Более 30 пакетов, не перечисленных здесь (FTP, LoRa, расширения Office, драйверы протоколов и другие), доступны в [организации NewLifeX](https://github.com/NewLifeX) и на [newlifex.com](https://newlifex.com).

---

## Команда

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

Команда NewLife основана в 2002 году и более двух десятилетий работает в экосистеме .NET и IoT, поддерживает около 100 проектов с открытым кодом (34 из них — основные) и предлагает интегрированные программно-аппаратные решения для энергетики, логистики, здравоохранения, промышленной автоматизации и интернет-отрасли. Мы следуем девизу «в учении нет старших — мастерство приносит уважение».

Open-source компоненты скачали <!--nuget-total:start-->6.48M<!--nuget-total:end--> раз с NuGet (по всей экосистеме, по состоянию на 2026-10).

Сайт: https://newlifex.com · Организация open source: https://github.com/NewLifeX

<details>
<summary>Официальный аккаунт WeChat: 智能大石头 (отсканируйте, чтобы подписаться)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
