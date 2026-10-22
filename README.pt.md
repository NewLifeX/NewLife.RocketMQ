# NewLife.RocketMQ - Cliente RocketMQ empresarial totalmente gerenciado

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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> Esta tradução pode estar desatualizada em relação à [versão em inglês](README.md).

**Um cliente RocketMQ empresarial totalmente gerenciado** com suporte a `.NET Framework 4.5+` / `.NET Standard 2.0+` / `.NET Core` / `.NET 5+`.  
**Implementado inteiramente em C#, sem dependências externas (nenhuma biblioteca de terceiros Java, gRPC ou Protobuf é necessária).**

---

## Visão geral do produto

NewLife.RocketMQ é um **cliente RocketMQ empresarial totalmente gerenciado** desenvolvido pela equipe NewLife e projetado para o ecossistema .NET. Ele suporta tanto o **protocolo Remoting do RocketMQ (Broker 4.x/5.x)** quanto o **protocolo gRPC Proxy (Proxy 5.x)**, cobrindo todas as capacidades essenciais de produtores e consumidores, além de recursos empresariais, com adaptadores unificados para Alibaba Cloud, Huawei Cloud, Tencent Cloud e o sistema de autenticação ACL do Apache.

**Vantagens principais**:

| Recurso | Descrição |
|---------|-------------|
| **Suporte a dois protocolos** | Remoting (maduro e estável no 4.x) + gRPC (voltado ao futuro no 5.x), com roteamento automático |
| **Zero dependências externas** | Codec de campos Protobuf embutido (escrito à mão sobre SpanWriter/SpanReader do NewLife.Core), sem exigir runtime Java ou gRPC |
| **Suporte a múltiplas nuvens** | Interface unificada `ICloudProvider` com adaptadores embutidos para Alibaba Cloud, Huawei Cloud, Tencent Cloud e Apache ACL |
| **Pronto para produção** | Recursos empresariais completos: nova tentativa de consumo, fila de mensagens mortas, verificação de transações, consumo ordenado, consumo Pop e mais |
| **Maior cobertura de frameworks** | Do .NET Framework 4.5+ ao .NET 10; os recursos gRPC estão disponíveis a partir do .NET Standard 2.1 |
| **Compatível com AOT** | O alvo net10.0 suporta NativeAOT/trimming: codificação/decodificação de quadros, serialização JSON e compactação de mensagens não dependem de reflexão em tempo de execução. O repositório inclui um teste de fumaça AOT e um teste de fumaça AOT de consumo do pacote NuGet; a CI valida a publicação e a execução AOT em uma matriz de RIDs mainstream (win/linux/osx, musl e Loongson LA64) |
| **Localização em tempo de execução** | Logs, exceções de análise de protocolo, exceções de API pública e descrições de configuração usam o inglês como texto de origem, com listas de palavras embutidas para 31 idiomas (132 entradas), traduzidas automaticamente conforme `Localization.Culture` com fallback para o inglês; logs com prefixo são traduzidos antes da composição do prefixo, e o prefixo não participa da busca na lista de palavras |
| **Alto desempenho** | Construído sobre a camada de rede de alto desempenho NewLife.Net: reutilização de conexões, canal VIP, compactação de mensagens e controle de concorrência |

---

## Instalação

```powershell
# Gerenciador de Pacotes NuGet
Install-Package NewLife.RocketMQ

# .NET CLI
dotnet add package NewLife.RocketMQ
```

```xml
<!-- PackageReference -->
<PackageReference Include="NewLife.RocketMQ" Version="3.0.*" />
```

---

## Início rápido

### Envio de mensagens

```csharp
using NewLife.RocketMQ;

var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    Group = "producer_group"
};
producer.Start();

// Envio síncrono
var result = producer.Publish("Hello RocketMQ!");
Console.WriteLine($"Message ID: {result.MsgId}");

// Envio assíncrono
await producer.PublishAsync("async message");

// Envio em lote
await producer.PublishBatch(new[] { "message1", "message2", "message3" });
```

### Consumo de mensagens

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
    return true; // Retornar true indica consumo bem-sucedido
};

consumer.Start();
```

### Acesso via gRPC (Proxy 5.x)

```csharp
// Produtor: definir GrpcProxyAddress encaminha o tráfego pelo caminho gRPC do 5.x (requer netstandard2.1+)
var producer = new Producer
{
    Topic = "test_topic",
    Group = "producer_group",
    NameServerAddress = "127.0.0.1:9876",
    GrpcProxyAddress = "http://127.0.0.1:8081",
};
producer.Start();
await producer.PublishAsync("gRPC message");

// Consumidor: as assinaturas são reportadas automaticamente na inicialização (conexão longa Telemetry); recebimento e confirmação trafegam por gRPC
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
// ... processamento de negócio ...
await consumer.AckMessageViaGrpcAsync("test_topic",
[
    .. messages.Select(m => new AckMessageEntry
    {
        ReceiptHandle = m.SystemProperties?.ReceiptHandle,
        MessageId = m.SystemProperties?.MessageId,
    }),
]);
```

> O consumo com long-polling também pode usar `GrpcPushConsumer` diretamente (semântica PUSH_CONSUMER com Ack e nova tentativa automáticos embutidos).
>
> **Nova tentativa e mensagens mortas do POP 5.x**: as mensagens que falham no consumo são gravadas pelo Broker em `%RETRY%{group}_{topic}` e, conforme o `popFromRetryProbability`, mescladas de volta na resposta de pull do tópico original; o cliente só precisa consumir o tópico original. Quando a contagem de entregas atinge `GrpcPushConsumer.MaxDeliveryAttempts` (padrão 16), a mensagem é movida para `%DLQ%{group}`; em cenários de recebimento manual, chame `Consumer.ForwardToDeadLetterQueueViaGrpcAsync` (`GetGrpcDlqTopic()` retorna o nome do tópico de mensagens mortas). O limite inferior do tempo invisível é de 10 segundos (convergido automaticamente dentro da biblioteca); valores menores são rejeitados pelo Broker.
>
> As mensagens mortas devem ser processadas em uma única passagem: `dotnet run --file scripts/DlqTool.cs -- --group <consumer group> --to <original topic>` republica as mensagens da fila de mensagens mortas no tópico especificado e confirma apenas as que foram republicadas com sucesso. O pull de mensagens mortas avança o offset de consumo, e as mensagens não confirmadas são movidas pelo Broker para `%RETRY%{group}_%DLQ%{group}`; portanto, a navegação somente leitura da fila de mensagens mortas não é suportada.
>
> **Mensagens ordenadas FIFO e mensagens transacionais**: o tipo do tópico deve corresponder ao tipo da mensagem; crie-o com `CreateTopic(topic, queueNum, 0, TopicMessageType.Fifo/Transaction)`. Para envios FIFO, defina `Message.MessageGroup`; no consumidor, defina `GrpcPushConsumer.Orderly = true` para processar em série e preservar a ordem. Para mensagens transacionais, use `PublishTransactionViaGrpcAsync` para enviar a half message e `EndTransactionViaGrpcAsync` para confirmar ou reverter (se não for confirmada, o Broker a reverte após um timeout).

### Mensagens com atraso

```csharp
// 18 níveis de atraso predefinidos
producer.PublishDelay("delayed message", DelayTimeLevels.s30);

// O modo gRPC suporta atrasos arbitrários (requer netstandard2.1+)
producer.GrpcProxyAddress = "http://127.0.0.1:8081";
await producer.PublishDelayViaGrpcAsync("arbitrary delay", DateTime.Now.AddMinutes(30));
```

### Mensagens transacionais

```csharp
var producer = new Producer
{
    Topic = "tx_topic",
    Group = "tx_group",
    NameServerAddress = "127.0.0.1:9876"
};

// Callback de verificação de transação
producer.OnCheckTransaction = (msg, transactionId) =>
{
    var success = CheckLocalTransaction(transactionId);
    return success ? TransactionState.Commit : TransactionState.Rollback;
};
producer.Start();

// Enviar half message -> executar transação local -> commit/rollback
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

### Mensagens ordenadas

```csharp
// Mensagens com a mesma chave vão para a mesma fila
var queue = producer.SelectQueue("order_123");
producer.Publish("ordered message 1", queue);
producer.Publish("ordered message 2", queue);

// Habilita o consumo ordenado no consumidor
consumer.OrderConsume = true;
```

### Padrão Request-Reply

```csharp
// O produtor envia uma requisição (síncrona/assíncrona)
var response = producer.Request("request message", timeout: 5000);
var reply = await producer.RequestAsync("async request", timeout: 5000);

// O consumidor responde
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

### Conversão de mensagens entre protocolos

```csharp
// Converte uma mensagem Remoting em mensagem gRPC (requer netstandard2.1+)
var grpcMsg = message.ToGrpcMessage();

// Converte uma mensagem gRPC em mensagem Remoting estendida / mensagem básica
var ext = grpcMsg.ToMessageExt();   // MessageExt (inclui todas as propriedades de sistema)
var msg = grpcMsg.ToMessage();      // Message (campos básicos)
```

---

## Recursos avançados do consumidor

### Nova tentativa de consumo e fila de mensagens mortas

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    EnableRetry = true,           // Habilita nova tentativa de consumo
    MaxReconsumeTimes = 3         // Número máximo de tentativas; mensagens excedentes entram na fila de mensagens mortas %DLQ%
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        try { ProcessMessage(msg); }
        catch { return false; } // Retornar false dispara uma nova tentativa
    }
    return true;
};
```

### Filtragem por Tag / SQL92

```csharp
// Filtragem por Tag
consumer.Tags = "TagA || TagB";

// Filtragem por expressão SQL92
consumer.ExpressionType = "SQL92";
consumer.Subscription = "age > 18 AND city = 'Shanghai'";
```

### Assinatura de múltiplos tópicos

```csharp
consumer.Topics = "topic1;topic2;topic3";
```

### Modo de consumo Pop

```csharp
// Consumo Pop (confirmação manual)
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
        await consumer.ChangeInvisibleTimeAsync(msg, 30000); // Estende o tempo de processamento
    }
}
```

### Limitação de consumo / Canal VIP / Compactação de mensagens

```csharp
consumer.MaxConcurrentConsume = 10; // Processa no máximo 10 mensagens simultaneamente

producer.VipChannelEnabled = true;  // Habilita o canal VIP (BrokerPort - 2)
producer.CompressOverBytes = 4096;  // Compacta automaticamente com ZLIB corpos maiores que 4KB
```

### HTTP Bridge (MQ -> WebAPI)

Para encaminhar as mensagens consumidas a um WebAPI, ou expor um WebAPI para publicação externa de mensagens, você pode combinar as APIs existentes; consulte o [Guia do HTTP Bridge](Doc/HttpBridge_Guide.md).

---

## Acesso a provedores de nuvem

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
        InstanceId = "MQ_INST_xxx"  // Opcional; analisado automaticamente a partir do endereço
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

### Autenticação ACL do Apache RocketMQ

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

## Publicação NativeAOT

Apenas o alvo `net10.0` suporta NativeAOT/trimming (frameworks de destino mais antigos não têm conceito de trimming e se comportam sem alterações). O pacote já inclui acessadores de membros gerados em código-fonte e raízes de metadados, portanto nenhuma configuração extra é necessária ao consumi-lo:

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

Cenários verificados (medidos com executáveis nativos): codificação/decodificação de quadros, dicionários de propriedades do cabeçalho de requisição, serialização JSON, compactação de mensagens (ida e volta ZLIB e limites de descompactação), propriedades LMQ, assinatura ACL, adaptadores de provedores de nuvem, conversão entre os dois protocolos e envio/recebimento ponta a ponta (corpo normal/corpo de objeto/lote/atraso/compactação de mensagens grandes, consumo, consulta de offset, bloqueio ordenado, POP), além do caminho gRPC 5.x (roteamento, confirmação de envio/recebimento, consumo push, encaminhamento para mensagens mortas, consumo ordenado FIFO).

Cobertura da CI: win-x64 (incluindo o ponta a ponta completo com RocketMQ local), linux-x64, linux-arm64, win-arm64, osx-x64/osx-arm64, linux-musl-x64/arm64 são publicados e executados nativamente; o linux-loongarch64 (Loongson) é executado via emulação QEMU em modo usuário mais o SDK Loongson (atualmente não há suporte oficial a NativeAOT para essa plataforma, então um teste de fumaça JIT de engenharia no estilo AOT é usado como critério).

**Corpos de objeto de tipos personalizados devem ser registrados**: quando um tipo personalizado (POCO) é usado como corpo de mensagem, o trimmer remove suas propriedades públicas por padrão, e o corpo do objeto é serializado silenciosamente como um objeto vazio `{}`. Registre cada tipo usado como corpo de mensagem uma vez na inicialização da aplicação:

```csharp
MqAot.RegisterBodyType<MyOrder>();
MqAot.RegisterBodyType<MyEvent>();
```

`MqAot.RegisterBodyType<T>()` só tem semântica de preservação para trimming no net10.0; nos demais alvos é uma operação nula (no-op), portanto projetos multi-target podem compartilhar o mesmo código de inicialização. Você também pode declarar suas próprias raízes de metadados usando o mesmo padrão da biblioteca (`[DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MyOrder))]` + `[ModuleInitializer]`).

---

## Visão geral da arquitetura

```
MqBase (classe base de negócio)
├── Producer
└── Consumer

Camada de comunicação
├── Protocolo Remoting (Broker 4.x/5.x)
│   ├── ClusterClient (conexão longa TCP, reutilização de Opaque)
│   ├── NameClient (descoberta de rotas, polling de 30s)
│   └── BrokerClient (heartbeat/cancelamento de registro)
│
└── Protocolo gRPC (Proxy 5.x, netstandard2.1+)
    ├── GrpcClient (HTTP/2, Unary + Server Streaming + conexão longa somente escrita)
    ├── GrpcMessagingService (11 métodos RPC)
    ├── GrpcTelemetrySession (conexão longa de reporte de recursos: reenvio periódico + reconexão ao desconectar)
    ├── MessageConverter (conversão de mensagens Remoting ↔ gRPC)
    └── Codec de campos (Protobuf escrito à mão sobre SpanWriter/SpanReader)

Camada de adaptadores de provedores de nuvem
├── AliyunProvider  (Alibaba Cloud: roteamento por ID de instância + NameServer HTTP)
├── HuaweiProvider  (Huawei Cloud: SSL/TLS + roteamento por ID de instância)
├── TencentProvider (Tencent Cloud: roteamento por prefixo Namespace)
└── AclProvider     (Apache ACL: assinatura HMAC-SHA1)
```

Consulte o [Documento de requisitos](Doc/需求文档.md), a [Lista de funcionalidades](Doc/功能清单.md), o [Design de arquitetura](Doc/架构设计.md), a [Análise de concorrentes](Doc/竞品分析.md), o [Guia do HTTP Bridge](Doc/HttpBridge_Guide.md) e a [Localização](Doc/多语言本地化.md).

---

## Visão geral dos recursos

### Produtor

| Recurso | Status | Descrição |
|---------|:------:|-------------|
| Envio síncrono/assíncrono/unidirecional | ✅ | Publish / PublishAsync / PublishOneway |
| Envio de mensagens em lote | ✅ | PublishBatch, agrupando várias mensagens em uma única requisição |
| Mensagens com atraso | ✅ | 18 níveis predefinidos + atrasos arbitrários via gRPC |
| Mensagens transacionais | ✅ | Half message + commit/rollback + callback de verificação |
| Mensagens ordenadas | ✅ | Envio para um MessageQueue especificado |
| Request-Reply | ✅ | Requisição-resposta síncrona/assíncrona |
| Compactação de mensagens | ✅ | ZLIB automático acima do limite CompressOverBytes |
| Rastreamento de mensagens | ✅ | AsyncTraceDispatcher + MessageTraceHook |

### Consumidor

| Recurso | Status | Descrição |
|---------|:------:|-------------|
| Consumo por pull / agendamento de consumo | ✅ | Pull em long-polling com atribuição automática de filas |
| Consumo em cluster/broadcast | ✅ | Atribuição média por Rebalance / persistência local de offset |
| Filtragem por Tag / SQL92 | ✅ | Filtragem por expressão |
| Assinatura de múltiplos tópicos | ✅ | Propriedade Topics; Rebalance por tópico |
| Nova tentativa de consumo + fila de mensagens mortas | ✅ | EnableRetry + MaxReconsumeTimes |
| Consumo ordenado | ✅ | Bloqueio de fila (OrderConsume) |
| Consumo Pop | ✅ | Pop/Ack/BatchAck/ChangeInvisibleTime |
| Limitação de consumo | ✅ | Controle por semáforo MaxConcurrentConsume |

### Gerenciamento e operações

| Recurso | Status | Descrição |
|---------|:------:|-------------|
| CRUD de tópicos/grupos de consumidores | ✅ | Criar/atualizar/excluir |
| Consulta de mensagens | ✅ | Por ID / por Key |
| Estatísticas de consumo / informações do cluster | ✅ | GetConsumeStats / GetClusterInfo |
| Gerenciamento e redefinição de offset | ✅ | Consultar/atualizar/redefinir |

### Protocolos e compatibilidade

| Versão do servidor | Remoting | gRPC | Descrição |
|----------------|:--------:|:----:|-------------|
| RocketMQ 4.0 ~ 4.9 | ✅ | — | Totalmente compatível |
| RocketMQ 5.x (Broker) | ✅ | — | Remoting é retrocompatível |
| RocketMQ 5.x (Proxy) | — | ✅ | Habilitado via GrpcProxyAddress |
| Alibaba Cloud 4.x | ✅ | — | Adaptador AliyunProvider |
| Huawei Cloud DMS | ✅ | — | Adaptador HuaweiProvider |
| Tencent Cloud TDMQ | ✅ | — | Adaptador TencentProvider |

---

## Cobertura de testes

A suíte de testes xUnit cobre a funcionalidade principal, recursos avançados, compatibilidade de protocolos, adaptadores de provedores de nuvem e testes de integração ponta a ponta (os casos de integração gRPC são ignorados automaticamente quando nenhum Proxy está configurado); ela também verifica a semântica de autenticação do Apache ACL 2.0 tanto no Remoting quanto no gRPC (requisições sem credenciais, de usuários não administradores ou com chaves erradas são rejeitadas, enquanto administradores podem enviar e receber normalmente). Os testes unitários são executados em paralelo por classe de teste, e `dotnet test XUnitTestRocketMQ/XUnitTest.csproj --filter "Category!=Integration"` conclui em cerca de 4 segundos; os casos de integração que dependem de um Broker real ficam na coleção RocketMQ e são executados em série para evitar interferência de tópicos e grupos de consumidores compartilhados; defina `ROCKETMQ_TEST_VERBOSE=1` para restaurar os logs do framework ao solucionar problemas. A CI é executada como uma matriz de RIDs: os testes unitários e os testes de fumaça AOT cobrem win-x64 / linux-x64 / linux-arm64 / Loongson LA64 (emulação QEMU, obrigatório), enquanto win-arm64, as duas arquiteturas osx e as duas arquiteturas linux-musl estão em período de observação; todos suportam acionamento manual.

Também incluídos: testes de fumaça NativeAOT (`AotSmoke`, 33 casos, incluindo compactação de mensagens, LMQ, assinatura ACL, caminho gRPC, encaminhamento para mensagens mortas e consumo ordenado FIFO), testes de fumaça AOT de consumo do pacote NuGet (`AotPackageSmoke`, consumindo os artefatos empacotados via PackageReference, cobrindo assets do pacote, serialização gerada em código-fonte, compactação e envio/recebimento ponta a ponta), coleta de linha de base de publicação (`scripts/AotBaseline.cs`, reportando tamanho/overhead de inicialização/vazão com verificações de limite e gravando no resumo do pipeline na CI), benchmarks de comparação de desempenho AOT/JIT (o projeto `Benchmark` baseado em BenchmarkDotNet, incluindo estatísticas de alocação; o lado AOT usa amostragem em múltiplas rodadas via `AotSmoke --bench`), uma ferramenta de operação de mensagens mortas (`scripts/DlqTool.cs`, republicando mensagens de `%DLQ%{group}` em um tópico especificado) e um script de aceitação de estabilidade gRPC (`scripts/GrpcStability.cs`, executado à noite na CI com uma reinicialização de Proxy injetada).

### Testes de integração locais (Windows)

```powershell
# Instalação e inicialização em um passo do NameServer + Broker + Proxy (o store é limpo a cada inicialização)
dotnet run --file scripts/RocketMqSetup.cs

$env:ROCKETMQ_NAMESERVER = "127.0.0.1:9876"
dotnet test XUnitTestRocketMQ\XUnitTest.csproj -c Release

# Parar / status
dotnet run --file scripts/RocketMqSetup.cs -- --stop
dotnet run --file scripts/RocketMqSetup.cs -- --status
```

A inicialização local usa o modo de baixo recurso por padrão: os logs das três JVMs são reduzidos a WARN (não mais gravados em `%USERPROFILE%\logs\rocketmqlogs`), o intervalo de flush assíncrono é de 5 segundos, o commitlog tem 64MB, os pools de threads são reduzidos e o store é limpo a cada inicialização para que o Broker esteja sempre em estado limpo. Ao solucionar problemas, use `--log-level INFO` (ou `$env:ROCKETMQ_TEST_LOG_LEVEL="INFO"`) para restaurar logs detalhados (o volume de logs aumenta consideravelmente); quando o gRPC não for necessário, use `--no-proxy` para iniciar uma JVM a menos; para apontar o store para um disco RAM, use `--store-dir R:\RocketMQ\store`.

---

## Contribuição

Issues e Pull Requests são bem-vindos!

1. Faça um fork deste repositório
2. Crie uma branch de recurso (`git checkout -b feature/AmazingFeature`)
3. Faça commit das suas alterações (`git commit -m 'Add some AmazingFeature'`)
4. Envie para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

---

## Licença

Este projeto é distribuído sob a [Licença MIT](LICENSE).

---

## Mapa do ecossistema

NewLife é um ecossistema em camadas de projetos open source versionados de forma independente. O mapa abaixo é compartilhado literalmente por todos os repositórios NewLife; cada projeto tem links para o seu repositório e para o seu pacote principal no NuGet.

| Projeto | O que faz | Downloads |
| --- | --- | --- |
| **Base** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | Log, configuração, cache, rede, serialização, agendamento e rastreamento APM. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | Middleware de dados: tabelas únicas na escala de dezenas de bilhões de linhas, particionamento automático, separação de leitura e escrita, mais de 10 bancos de dados. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | Biblioteca de rede de alto desempenho: 1,77M pps / 14,5 Gbps em uma máquina (TCP 1KB, 2026-10), 4M de conexões TCP persistentes. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | Framework de comunicação por protocolo e RPC para aplicações cliente/servidor e dispositivos IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | Instala aplicações .NET como daemons do sistema operacional: serviços do Windows e systemd do Linux. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | Modelos de projeto `dotnet new` para console, web, API, serviço, dados e IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **Dados e mensagens** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Cliente Redis com latência em microssegundos, milhões de operações por segundo e filas de mensagens integradas. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Cliente RocketMQ totalmente gerenciado para Apache RocketMQ e Alibaba Cloud Message Queue. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | Cliente e servidor MQTT, incluindo conectividade com o Alibaba Cloud IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | Banco de dados híbrido em C#: embarcado ou em rede, relacional, séries temporais, fila de mensagens e KV em um só. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | Driver MySql e camada auxiliar construídos sobre a pilha de dados NewLife. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web, IA e distribuído** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | Framework de plataforma administrativa rápida: permissões, SSO, servidor OAuth e scaffolding CRUD. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | Plataforma de serviços distribuídos: gestão de nós, APM, centro de configuração, registro e centro de publicação. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | Plataforma distribuída de tarefas e agendamento de dados em tempo real, com particionamento, repetição e reprocessamento. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | Cliente unificado para 46 provedores de LLM, além do ChatAI, uma aplicação web de chat pronta para uso. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **Industrial e IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | Biblioteca padrão de IoT que define contratos de protocolo e abstrações de dispositivos. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | Implementação Modbus TCP/RTU/ASCII sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | Protocolo Siemens PLC sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | Protocolo Omron PLC sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | Protocolo Mitsubishi Melsec PLC sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | Cliente OPC para acesso a dados industriais. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | Pilha do protocolo de automação predial BACnet. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | Plataforma IoT de código aberto leve, com gestão de gateways e dispositivos. | — |
| **Telemática veicular** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | Protocolo de posicionamento veicular JT/T 808: mais de 70 corpos de mensagem, 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | Protocolo plataforma-a-plataforma JT/T 809 para autoridades de transporte. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | Vídeo veicular JT/T 1078: análise RTP, remux para FLV/fMP4 e transmissão ao vivo. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **Componentes de domínio** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Um pacote para Excel, Word, PPT, PDF e mais de 15 formatos de documento. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | Áudio multiplataforma: codecs, E/S de dispositivos, DSP, contêineres e streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | Codecs de vídeo, formatos de contêiner, processamento de frames e streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | Interface unificada sobre Baidu, AMap, Tencent e Tianditu, com geocodificação e conversão de coordenadas. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | Geolocalização IPv4 com banco de dados offline mapeado em memória e API online como alternativa. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **Ferramentas e produtos** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | Caixa de ferramentas do desenvolvedor: rede, porta serial, criptografia, regex, Modbus e MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | Sistema auto-hospedado de gerenciamento de código-fonte Git. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | Instala automaticamente o runtime .NET correspondente quando iniciado em um host mais antigo. | — |

> Os badges mostram o pacote principal de cada projeto; pacotes de tema, extensão e satélites estão listados nas respectivas páginas do NuGet. Os mais de 30 pacotes não listados aqui (FTP, LoRa, extensões do Office, drivers de protocolo e outros) estão disponíveis na [organização NewLifeX](https://github.com/NewLifeX) e em [newlifex.com](https://newlifex.com).

---

## Equipe

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

Fundada em 2002, a equipe NewLife dedica-se há mais de duas décadas ao ecossistema .NET e de IoT, mantém cerca de 100 projetos de código aberto (34 deles principais) e oferece soluções integradas de software e hardware para os setores de energia, logística, saúde, controle industrial e internet. Seguimos o lema "não há hierarquia no aprendizado — o domínio conquista o respeito".

Os componentes de código aberto já foram baixados <!--nuget-total:start-->6.48M<!--nuget-total:end--> vezes no NuGet (todo o ecossistema, até 2026-10).

Website: https://newlifex.com · Organização de código aberto: https://github.com/NewLifeX

<details>
<summary>Conta oficial no WeChat: 智能大石头 (escaneie para seguir)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>


