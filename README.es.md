# NewLife.RocketMQ - Cliente RocketMQ empresarial totalmente administrado

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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> Esta traducción puede estar desactualizada; consulte la [versión en inglés](README.md).

**Un cliente RocketMQ empresarial, totalmente administrado**, compatible con `.NET Framework 4.5+` / `.NET Standard 2.0+` / `.NET Core` / `.NET 5+`.  
**Implementado íntegramente en C#, sin dependencias externas (no se requieren bibliotecas de terceros de Java, gRPC ni Protobuf).**

---

## Descripción general del producto

NewLife.RocketMQ es un **cliente RocketMQ empresarial, totalmente administrado** desarrollado por el equipo NewLife y diseñado para el ecosistema .NET. Admite tanto el **protocolo Remoting de RocketMQ (Broker 4.x/5.x)** como el **protocolo gRPC Proxy (Proxy 5.x)**, cubriendo todas las capacidades principales de productor y consumidor, así como funciones empresariales, con adaptadores unificados para Alibaba Cloud, Huawei Cloud, Tencent Cloud y el sistema de autenticación Apache ACL.

**Ventajas principales**:

| Característica | Descripción |
|---------|-------------|
| **Soporte de doble protocolo** | Remoting (maduro y estable en 4.x) + gRPC (orientado al futuro en 5.x), con enrutamiento automático |
| **Sin dependencias externas** | Códec de campos Protobuf integrado (escrito a mano sobre SpanWriter/SpanReader de NewLife.Core), sin necesidad de runtime de Java ni de gRPC |
| **Soporte multicloud** | Interfaz unificada `ICloudProvider` con adaptadores integrados para Alibaba Cloud, Huawei Cloud, Tencent Cloud y Apache ACL |
| **Listo para producción** | Funciones empresariales completas: reintento de consumo, cola de mensajes muertos, verificación de transacciones, consumo ordenado, consumo Pop y más |
| **Bus de eventos** | Bus de eventos integrado basado en el contrato `IEventBus` de Core: los eventos se publican y suscriben entre procesos mediante Producer/Consumer, con reintento y envío a la cola de mensajes muertos automáticos |
| **Máxima cobertura de frameworks** | Desde .NET Framework 4.5+ hasta .NET 10; las funciones gRPC están disponibles en .NET Standard 2.1+ |
| **Compatible con AOT** | El destino net10.0 admite NativeAOT/recorte: la codificación y decodificación de tramas, la serialización JSON y la compresión de mensajes no dependen de reflexión en tiempo de ejecución. El repositorio incluye una prueba de humo AOT y una prueba de humo AOT de consumo del paquete NuGet; la CI valida la publicación y ejecución AOT en una matriz de RID principales (win/linux/osx, musl y Loongson LA64) |
| **Localización en tiempo de ejecución** | Los logs, las excepciones de análisis de protocolo, las excepciones de API pública y las descripciones de configuración usan el inglés como texto de origen, con listas de palabras integradas para 31 idiomas, traducidas automáticamente según `Localization.Culture` con retroceso al inglés; los logs con prefijo se traducen antes de componer el prefijo, y el prefijo no participa en la búsqueda en la lista de palabras |
| **Alto rendimiento** | Basado en la capa de red de alto rendimiento NewLife.Net: reutilización de conexiones, canal VIP, compresión de mensajes y control de concurrencia |

---

## Instalación

```powershell
# Administrador de paquetes NuGet
Install-Package NewLife.RocketMQ

# CLI de .NET
dotnet add package NewLife.RocketMQ
```

```xml
<!-- PackageReference -->
<PackageReference Include="NewLife.RocketMQ" Version="4.0.*" />
```

---

## Inicio rápido

### Envío de mensajes

```csharp
using NewLife.RocketMQ;

var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    Group = "producer_group"
};
producer.Start();

// Envío síncrono
var result = producer.Publish("Hello RocketMQ!");
Console.WriteLine($"Message ID: {result.MsgId}");

// Envío asíncrono
await producer.PublishAsync("async message");

// Envío por lotes
await producer.PublishBatch(new[] { "message1", "message2", "message3" });
```

### Recepción de mensajes

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
    return true; // Devolver true indica que el consumo se realizó correctamente
};

consumer.Start();
```

### Acceso gRPC (5.x Proxy)

```csharp
// Producer: establecer GrpcProxyAddress enruta el tráfico por la ruta gRPC de 5.x (requiere netstandard2.1+)
var producer = new Producer
{
    Topic = "test_topic",
    Group = "producer_group",
    NameServerAddress = "127.0.0.1:9876",
    GrpcProxyAddress = "http://127.0.0.1:8081",
};
producer.Start();
await producer.PublishAsync("gRPC message");

// Consumer: las relaciones de suscripción se reportan automáticamente al iniciar (conexión persistente de Telemetry); la recepción y el acuse van por gRPC
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
// ... procesamiento de negocio ...
await consumer.AckMessageViaGrpcAsync("test_topic",
[
    .. messages.Select(m => new AckMessageEntry
    {
        ReceiptHandle = m.SystemProperties?.ReceiptHandle,
        MessageId = m.SystemProperties?.MessageId,
    }),
]);
```

> El consumo por long-polling también puede usar `GrpcPushConsumer` directamente (semántica PUSH_CONSUMER con Ack automático y reintento integrados).
>
> **Reintento POP y mensajes muertos en 5.x**: los mensajes que fallan en el consumo son escritos por el Broker en `%RETRY%{group}_{topic}` y, según `popFromRetryProbability`, se vuelven a integrar en la respuesta de pull del topic original; el cliente solo necesita consumir el topic original. Cuando el número de entregas alcanza `GrpcPushConsumer.MaxDeliveryAttempts` (16 por defecto), el mensaje se mueve a `%DLQ%{group}`; en escenarios de recepción manual, llame a `Consumer.ForwardToDeadLetterQueueViaGrpcAsync` (`GetGrpcDlqTopic()` devuelve el nombre del topic de mensajes muertos). El límite inferior del tiempo de invisibilidad es de 10 segundos (ajustado automáticamente dentro de la biblioteca); los valores menores son rechazados por el Broker.
>
> Los mensajes muertos deben procesarse en una sola pasada: `dotnet run --file scripts/DlqTool.cs -- --group <grupo de consumidores> --to <topic original>` republica los mensajes de la cola de mensajes muertos en el topic indicado y solo confirma los que se republicaron correctamente. Extraer mensajes muertos avanza el offset de consumo, y los mensajes no confirmados son movidos por el Broker a `%RETRY%{group}_%DLQ%{group}`; por lo tanto, no se admite la consulta de solo lectura de la cola de mensajes muertos.
>
> **Mensajes ordenados FIFO y mensajes transaccionales**: el tipo de topic debe coincidir con el tipo de mensaje; créelo con `CreateTopic(topic, queueNum, 0, TopicMessageType.Fifo/Transaction)`. Para los envíos FIFO, establezca `Message.MessageGroup`; en el consumidor, establezca `GrpcPushConsumer.Orderly = true` para procesar de forma serial y preservar el orden. Para los mensajes transaccionales, use `PublishTransactionViaGrpcAsync` para enviar el medio mensaje y `EndTransactionViaGrpcAsync` para confirmar o revertir (si no se confirma, el Broker lo revierte tras un tiempo de espera).

### Mensajes diferidos

```csharp
// 18 niveles de retardo predefinidos
producer.PublishDelay("delayed message", DelayTimeLevels.s30);

// El modo gRPC admite retardos arbitrarios (requiere netstandard2.1+)
producer.GrpcProxyAddress = "http://127.0.0.1:8081";
await producer.PublishDelayViaGrpcAsync("arbitrary delay", DateTime.Now.AddMinutes(30));
```

### Mensajes transaccionales

```csharp
var producer = new Producer
{
    Topic = "tx_topic",
    Group = "tx_group",
    NameServerAddress = "127.0.0.1:9876"
};

// Devolución de llamada de verificación de transacción
producer.OnCheckTransaction = (msg, transactionId) =>
{
    var success = CheckLocalTransaction(transactionId);
    return success ? TransactionState.Commit : TransactionState.Rollback;
};
producer.Start();

// Enviar medio mensaje -> ejecutar la transacción local -> confirmar/revertir
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

### Mensajes ordenados

```csharp
// Los mensajes con la misma clave van a la misma cola
var queue = producer.SelectQueue("order_123");
producer.Publish("ordered message 1", queue);
producer.Publish("ordered message 2", queue);

// Habilitar el consumo ordenado en el consumidor
consumer.OrderConsume = true;
```

### Patrón Request-Reply

```csharp
// El productor envía una petición (síncrona/asíncrona)
var response = producer.Request("request message", timeout: 5000);
var reply = await producer.RequestAsync("async request", timeout: 5000);

// El consumidor responde
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

### Conversión de mensajes entre protocolos

```csharp
// Convertir un mensaje Remoting en un mensaje gRPC (requiere netstandard2.1+)
var grpcMsg = message.ToGrpcMessage();

// Convertir un mensaje gRPC en un mensaje Remoting extendido / mensaje básico
var ext = grpcMsg.ToMessageExt();   // MessageExt (incluye todas las propiedades del sistema)
var msg = grpcMsg.ToMessage();      // Message (campos básicos)
```

---

### Bus de eventos

```csharp
using NewLife.Messaging;
using NewLife.RocketMQ;

// Los topics de eventos pueden contener puntos; los nombres se normalizan a nombres válidos de RocketMQ (order.created -> order_created)
using var bus = new RocketMqEventBus<OrderPaidEvent>("127.0.0.1:9876", "order.created", "order_service");

// Suscripción: si el manejador lanza una excepción se reintenta la entrega y, al agotarse los intentos, el mensaje pasa a la cola de mensajes muertos
await bus.SubscribeAsync((OrderPaidEvent e, IEventContext ctx) =>
{
    Console.WriteLine($"Pedido pagado: {e.OrderId}");
    return Task.CompletedTask;
}, "billing");

// Publicación: se entrega a través del Broker; los suscriptores locales también la reciben mediante su grupo de consumidores
await bus.PublishAsync(new OrderPaidEvent { OrderId = 1001 });

// Integración con un hub de eventos: la fábrica crea y cachea un bus por topic
var hub = new EventHub<OrderPaidEvent>
{
    Factory = new RocketMqEventBusFactory("127.0.0.1:9876"),
};
```

> El grupo de consumidores toma por defecto la identidad del nodo actual, de modo que cada nodo recibe todos los eventos. Pasa el mismo clientId para cambiar a la semántica de consumidores competidores. Mantén los manejadores idempotentes, porque un fallo parcial de lote reentrega el lote completo.

---

## Funciones avanzadas del consumidor

### Reintento de consumo y cola de mensajes muertos

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    EnableRetry = true,           // Habilitar el reintento de consumo
    MaxReconsumeTimes = 3         // Número máximo de reintentos; los mensajes que lo exceden entran en la cola de mensajes muertos %DLQ%
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        try { ProcessMessage(msg); }
        catch { return false; } // Devolver false activa un reintento
    }
    return true;
};
```

### Filtrado por Tag / SQL92

```csharp
// Filtrado por Tag
consumer.Tags = "TagA || TagB";

// Filtrado por expresión SQL92
consumer.ExpressionType = "SQL92";
consumer.Subscription = "age > 18 AND city = 'Shanghai'";
```

### Suscripción a múltiples topics

```csharp
consumer.Topics = "topic1;topic2;topic3";
```

### Modo de consumo Pop

```csharp
// Consumo Pop (confirmación manual)
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
        await consumer.ChangeInvisibleTimeAsync(msg, 30000); // Ampliar el tiempo de procesamiento
    }
}
```

### Limitación de consumo / Canal VIP / Compresión de mensajes

```csharp
consumer.MaxConcurrentConsume = 10; // Procesar como máximo 10 mensajes de forma concurrente

producer.VipChannelEnabled = true;  // Habilitar el canal VIP (BrokerPort - 2)
producer.CompressOverBytes = 4096;  // Comprimir automáticamente con ZLIB los cuerpos de mensaje mayores de 4 KB
```

### Puente HTTP (MQ -> WebAPI)

Para reenviar los mensajes consumidos a una WebAPI, o para exponer una WebAPI que permita la publicación externa de mensajes, puede combinar las API existentes; consulte la [Guía del puente HTTP](Doc/HttpBridge_Guide.md).

---

## Acceso a proveedores cloud

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
        InstanceId = "MQ_INST_xxx"  // Opcional; se analiza automáticamente a partir de la dirección
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

### Autenticación Apache RocketMQ ACL

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

## Publicación NativeAOT

Solo el destino `net10.0` admite NativeAOT/recorte (los frameworks de destino anteriores no tienen concepto de recorte y se comportan sin cambios). El paquete ya incluye accesores de miembros generados por código fuente y raíces de metadatos, por lo que no se requiere configuración adicional al consumirlo:

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

Escenarios verificados (medidos con ejecutables nativos): codificación y decodificación de tramas, diccionarios de propiedades de cabecera de petición, serialización JSON, compresión de mensajes (ida y vuelta ZLIB y límites de descompresión), propiedades LMQ, firma ACL, adaptadores de proveedores cloud, conversión entre protocolos y envío/recepción de extremo a extremo (cuerpo normal/cuerpo de objeto/lotes/diferidos/compresión de mensajes grandes, consumo, consulta de offset, bloqueo ordenado, POP), así como la ruta gRPC de 5.x (enrutamiento, envío/recepción con acuse, consumo push, reenvío a cola de mensajes muertos, consumo ordenado FIFO).

Cobertura de CI: win-x64 (incluida la prueba de extremo a extremo completa con RocketMQ local), linux-x64, linux-arm64, win-arm64, osx-x64/osx-arm64, linux-musl-x64/arm64 se publican y ejecutan de forma nativa; Loongson linux-loongarch64 se ejecuta mediante emulación en modo usuario de QEMU junto con el SDK de Loongson (actualmente no existe soporte oficial de NativeAOT para esa plataforma, por lo que se usa como criterio una prueba de humo JIT de ingeniería de estilo AOT).

**Los cuerpos de objeto de tipos personalizados deben registrarse**: cuando se usa un tipo personalizado (POCO) como cuerpo de mensaje, el recortador elimina sus propiedades públicas de forma predeterminada y el cuerpo del objeto se serializa silenciosamente como un objeto vacío `{}`. Registre una vez cada tipo usado como cuerpo de mensaje al iniciar la aplicación:

```csharp
MqAot.RegisterBodyType<MyOrder>();
MqAot.RegisterBodyType<MyEvent>();
```

`MqAot.RegisterBodyType<T>()` solo tiene semántica de preservación frente al recorte en net10.0; es una operación nula en otros destinos, por lo que los proyectos multi-target pueden compartir el mismo código de arranque. También puede declarar sus propias raíces de metadatos con el mismo patrón que la biblioteca (`[DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MyOrder))]` + `[ModuleInitializer]`).

---

## Descripción general de la arquitectura

```
MqBase (clase base de negocio)
├── Producer
└── Consumer

Capa de comunicación
├── Protocolo Remoting (Broker 4.x/5.x)
│   ├── ClusterClient (conexión persistente TCP, reutilización de Opaque)
│   ├── NameClient (descubrimiento de rutas, sondeo cada 30 s)
│   └── BrokerClient (latido/cancelación de registro)
│
└── Protocolo gRPC (5.x Proxy, netstandard2.1+)
    ├── GrpcClient (HTTP/2, Unary + Server Streaming + conexión persistente de solo escritura)
    ├── GrpcMessagingService (11 métodos RPC)
    ├── GrpcTelemetrySession (conexión persistente de reporte de recursos: reenvío periódico + reconexión al desconectarse)
    ├── MessageConverter (conversión de mensajes Remoting ↔ gRPC)
    └── Códec de campos (Protobuf escrito a mano sobre SpanWriter/SpanReader)

Capa de adaptadores de proveedores cloud
├── AliyunProvider  (Alibaba Cloud: enrutamiento por ID de instancia + NameServer HTTP)
├── HuaweiProvider  (Huawei Cloud: SSL/TLS + enrutamiento por ID de instancia)
├── TencentProvider (Tencent Cloud: enrutamiento con prefijo Namespace)
└── AclProvider     (Apache ACL: firma HMAC-SHA1)
```

Consulte el [documento de requisitos](Doc/需求文档.md), la [lista de funciones](Doc/功能清单.md), el [diseño de arquitectura](Doc/架构设计.md), el [análisis competitivo](Doc/竞品分析.md), la [Guía del puente HTTP](Doc/HttpBridge_Guide.md) y la [localización](Doc/多语言本地化.md).

---

## Descripción general de funciones

### Producer

| Función | Estado | Descripción |
|---------|:------:|-------------|
| Envío síncrono/asíncrono/unidireccional | ✅ | Publish / PublishAsync / PublishOneway |
| Envío de mensajes por lotes | ✅ | PublishBatch, fusiona varios mensajes en una sola petición |
| Mensajes diferidos | ✅ | 18 niveles predefinidos + retardos arbitrarios por gRPC |
| Mensajes transaccionales | ✅ | Medio mensaje + confirmación/reversión + devolución de llamada de verificación |
| Mensajes ordenados | ✅ | Envío a una MessageQueue concreta |
| Request-Reply | ✅ | Petición-respuesta síncrona/asíncrona |
| Compresión de mensajes | ✅ | ZLIB automático por encima del umbral CompressOverBytes |
| Traza de mensajes | ✅ | AsyncTraceDispatcher + MessageTraceHook |

### Consumer

| Función | Estado | Descripción |
|---------|:------:|-------------|
| Consumo por pull / planificación del consumo | ✅ | Pull por long-polling con asignación automática de colas |
| Consumo en clúster / difusión | ✅ | Asignación equilibrada por rebalanceo / persistencia local del offset |
| Filtrado por Tag / SQL92 | ✅ | Filtrado por expresión |
| Suscripción a múltiples topics | ✅ | Propiedad Topics; rebalanceo por topic |
| Reintento de consumo + cola de mensajes muertos | ✅ | EnableRetry + MaxReconsumeTimes |
| Consumo ordenado | ✅ | Bloqueo de cola (OrderConsume) |
| Consumo Pop | ✅ | Pop/Ack/BatchAck/ChangeInvisibleTime |
| Limitación del consumo | ✅ | Control por semáforo MaxConcurrentConsume |

### Bus de eventos

| Función | Estado | Descripción |
|---------|:------:|-------------|
| Contrato Core IEventBus | ✅ | RocketMqEventBus&lt;TEvent&gt; / RocketMqEventContext |
| Publicación/suscripción entre procesos | ✅ | Producer publica, el grupo de consumidores de Consumer se suscribe; los fallos van a reintento/cola de mensajes muertos |
| Fábrica de hub de eventos | ✅ | RocketMqEventBusFactory implementa IEventBusFactory y se conecta a EventHub.Factory |
| Normalización de nombres | ✅ | MqEventBusNaming: caracteres no válidos a guiones bajos, truncado a 127 caracteres |

### Gestión y operación

| Función | Estado | Descripción |
|---------|:------:|-------------|
| CRUD de topics / grupos de consumidores | ✅ | Crear/actualizar/eliminar |
| Consulta de mensajes | ✅ | Por ID / por Key |
| Estadísticas de consumo / información del clúster | ✅ | GetConsumeStats / GetClusterInfo |
| Gestión y restablecimiento de offsets | ✅ | Consultar/actualizar/restablecer |

### Protocolos y compatibilidad

| Versión del servidor | Remoting | gRPC | Descripción |
|----------------|:--------:|:----:|-------------|
| RocketMQ 4.0 ~ 4.9 | ✅ | — | Totalmente compatible |
| RocketMQ 5.x (Broker) | ✅ | — | Remoting es retrocompatible |
| RocketMQ 5.x (Proxy) | — | ✅ | Se habilita mediante GrpcProxyAddress |
| Alibaba Cloud 4.x | ✅ | — | Adaptador AliyunProvider |
| Huawei Cloud DMS | ✅ | — | Adaptador HuaweiProvider |
| Tencent Cloud TDMQ | ✅ | — | Adaptador TencentProvider |

---

## Cobertura de pruebas

El conjunto de pruebas xUnit cubre la funcionalidad principal, las funciones avanzadas, la compatibilidad de protocolos, los adaptadores de proveedores cloud y las pruebas de integración E2E (los casos de integración gRPC se omiten automáticamente cuando no hay un Proxy configurado); también verifica la semántica de autenticación de Apache ACL 2.0 tanto en Remoting como en gRPC (las peticiones sin credenciales, de usuarios que no son administradores o con claves incorrectas se rechazan, mientras que los administradores pueden enviar y recibir con normalidad). Las pruebas unitarias se ejecutan en paralelo por clase de prueba, y `dotnet test XUnitTestRocketMQ/XUnitTest.csproj --filter "Category!=Integration"` finaliza en unos 4 segundos; los casos de integración que dependen de un Broker real se agrupan en la colección RocketMQ y se ejecutan en serie para evitar interferencias de topics y grupos de consumidores compartidos; establezca `ROCKETMQ_TEST_VERBOSE=1` para restaurar los logs del framework al diagnosticar problemas. La CI se ejecuta como una matriz de RID: las pruebas unitarias y las pruebas de humo AOT cubren win-x64 / linux-x64 / linux-arm64 / Loongson LA64 (emulación QEMU, obligatorio), mientras que win-arm64, ambas arquitecturas de osx y ambas arquitecturas de linux-musl están en periodo de observación; todas admiten ejecución manual.

También se incluye: pruebas de humo NativeAOT (`AotSmoke`, que cubren compresión de mensajes, LMQ, firma ACL, ruta gRPC, reenvío a cola de mensajes muertos y consumo ordenado FIFO), pruebas de humo AOT de consumo del paquete NuGet (`AotPackageSmoke`, que consumen los artefactos empaquetados mediante PackageReference y cubren los recursos del paquete, la serialización generada por código fuente, la compresión y el envío/recepción de extremo a extremo), la recopilación de líneas base de publicación (`scripts/AotBaseline.cs`, que informa tamaño, sobrecarga de arranque y rendimiento con comprobaciones de umbral y escribe en el resumen de la canalización en CI), los benchmarks de comparación de rendimiento AOT/JIT (el proyecto `Benchmark` basado en BenchmarkDotNet, con estadísticas de asignación; el lado AOT usa muestreo multironda mediante `AotSmoke --bench`), una herramienta de operación de mensajes muertos (`scripts/DlqTool.cs`, que republica los mensajes de `%DLQ%{group}` en un topic especificado) y un script de aceptación de estabilidad gRPC (`scripts/GrpcStability.cs`, ejecutado cada noche en CI con un reinicio de Proxy inyectado).

### Pruebas de integración locales (Windows)

```powershell
# Instalación y arranque en un solo paso de NameServer + Broker + Proxy (el store se limpia en cada arranque)
dotnet run --file scripts/RocketMqSetup.cs

$env:ROCKETMQ_NAMESERVER = "127.0.0.1:9876"
dotnet test XUnitTestRocketMQ\XUnitTest.csproj -c Release

# Detener / estado
dotnet run --file scripts/RocketMqSetup.cs -- --stop
dotnet run --file scripts/RocketMqSetup.cs -- --status
```

El arranque local usa por defecto el modo de bajos recursos: los logs de las tres JVM se reducen a WARN (ya no se escriben en `%USERPROFILE%\logs\rocketmqlogs`), el intervalo de volcado asíncrono es de 5 segundos, el commitlog es de 64 MB, los pools de hilos se ajustan y el store se limpia en cada arranque para que el Broker esté siempre en un estado limpio. Al diagnosticar problemas, use `--log-level INFO` (o `$env:ROCKETMQ_TEST_LOG_LEVEL="INFO"`) para restaurar los logs detallados (el volumen de logs aumenta notablemente); cuando no se necesite gRPC, use `--no-proxy` para arrancar una JVM menos; para apuntar el store a un disco RAM, use `--store-dir R:\RocketMQ\store`.

---

## Contribución

¡Las issues y los Pull Requests son bienvenidos!

1. Haga un fork de este repositorio
2. Cree una rama de funcionalidad (`git checkout -b feature/AmazingFeature`)
3. Confirme sus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Suba la rama (`git push origin feature/AmazingFeature`)
5. Abra un Pull Request

---

## Licencia

Este proyecto se publica bajo la [Licencia MIT](LICENSE).

---

## Mapa del ecosistema

NewLife es un ecosistema por capas de proyectos de código abierto con versionado independiente. El mapa siguiente se comparte literalmente en todos los repositorios de NewLife; cada proyecto enlaza a su repositorio y a su paquete principal de NuGet.

| Proyecto | Qué hace | Descargas |
| --- | --- | --- |
| **Fundación** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | Registro de logs, configuración, caché, red, serialización, programación de tareas y seguimiento APM. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | Middleware de datos: tablas únicas a escala de decenas de miles de millones, particionado automático, separación lectura/escritura y más de 10 bases de datos. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | Biblioteca de red de alto rendimiento: 1,77 M pps / 14,5 Gbps en una máquina (TCP 1KB, 2026-10), 4 M conexiones TCP persistentes. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | Framework de comunicación por protocolo y RPC para aplicaciones cliente/servidor y dispositivos IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | Instala aplicaciones .NET como demonios del sistema operativo: servicios de Windows y systemd de Linux. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | Plantillas de proyecto `dotnet new` para consola, web, API, servicio, datos e IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **Datos y mensajería** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Cliente Redis con latencia de microsegundos, millones de operaciones por segundo y colas de mensajes integradas. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Cliente RocketMQ totalmente administrado para Apache RocketMQ y Alibaba Cloud Message Queue. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | Cliente y servidor MQTT, incluida la conectividad con Alibaba Cloud IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | Base de datos híbrida en C#: embebida o en red, relacional, series temporales, cola de mensajes y KV en una sola. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | Controlador y capa de ayuda de MySql construidos sobre la pila de datos de NewLife. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web, IA y distribuido** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | Framework rápido para plataformas de administración: permisos, SSO, servidor OAuth y andamiaje CRUD. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | Plataforma distribuida de servicios: gestión de nodos, APM, centro de configuración, registro y centro de despliegues. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | Plataforma de tareas distribuidas y programación de datos en tiempo real con particionado, reintentos y relleno histórico. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | Cliente unificado para 46 proveedores de LLM más ChatAI, una aplicación web de chat lista para usar. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **Industrial e IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | Biblioteca estándar de IoT que define contratos de protocolo y abstracciones de dispositivos. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | Implementación de Modbus TCP/RTU/ASCII sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | Protocolo para PLC Siemens sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | Protocolo para PLC Omron sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | Protocolo para PLC Mitsubishi Melsec sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | Cliente OPC para el acceso a datos industriales. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | Pila de protocolo BACnet para la automatización de edificios. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | Plataforma IoT de código abierto y ligera con gestión de pasarelas y dispositivos. | — |
| **Telemática vehicular** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | Protocolo de posicionamiento vehicular JT/T 808: más de 70 cuerpos de mensaje, 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | Protocolo plataforma a plataforma JT/T 809 para autoridades de transporte. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | Vídeo a bordo JT/T 1078: análisis RTP, remultiplexado FLV/fMP4 y transmisión en directo. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **Componentes de dominio** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Un solo paquete para Excel, Word, PPT, PDF y más de 15 formatos de documento. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | Audio multiplataforma: códecs, E/S de dispositivos, DSP, contenedores y streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | Códecs de vídeo, formatos de contenedor, procesamiento de fotogramas y streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | Interfaz unificada sobre Baidu, AMap, Tencent y Tianditu con geocodificación y conversión de coordenadas. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | Geolocalización IPv4 con una base de datos offline mapeada en memoria y una API en línea de respaldo. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **Herramientas y productos** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | Caja de herramientas del desarrollador: red, puerto serie, criptografía, expresiones regulares, Modbus y MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | Sistema de gestión de código fuente Git autoalojado. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | Instala automáticamente el runtime de .NET correspondiente cuando se ejecuta desde un host más antiguo. | — |

> Las insignias muestran el paquete principal de cada proyecto; los paquetes de temas, extensiones y satélites figuran en sus páginas de NuGet. Los más de 30 paquetes no listados aquí (FTP, LoRa, extensiones de Office, controladores de protocolo, etc.) están disponibles en la [organización NewLifeX](https://github.com/NewLifeX) y en [newlifex.com](https://newlifex.com).

---

## Equipo

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

NewLife se fundó en 2002 y lleva más de dos décadas dedicado al ecosistema .NET e IoT, mantiene cerca de 100 proyectos de código abierto (34 de ellos principales) y ofrece soluciones integradas de software y hardware para los sectores de energía, logística, salud, control industrial e Internet. Nos guía el lema «en el aprendizaje no hay antigüedad: quien domina es maestro».

Los componentes de código abierto acumulan <!--nuget-total:start-->6.48M<!--nuget-total:end--> descargas en NuGet (todo el ecosistema, a fecha de 2026-10).

Sitio web: https://newlifex.com · Organización de código abierto: https://github.com/NewLifeX

<details>
<summary>Cuenta oficial de WeChat: 智能大石头 (escanea para seguir)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
