# NewLife.RocketMQ - Client RocketMQ entièrement managé de qualité entreprise

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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> Cette traduction peut être en retard sur la [version anglaise](README.md).

**Un client RocketMQ de qualité entreprise, entièrement managé**, compatible `.NET Framework 4.5+` / `.NET Standard 2.0+` / `.NET Core` / `.NET 5+`.  
**Entièrement implémenté en C#, sans aucune dépendance externe (aucune bibliothèque tierce Java, gRPC ou Protobuf requise).**

---

## Aperçu du produit

NewLife.RocketMQ est un **client RocketMQ de qualité entreprise, entièrement managé**, développé par l'équipe NewLife et conçu pour l'écosystème .NET. Il prend en charge à la fois le **protocole Remoting de RocketMQ (Broker 4.x/5.x)** et le **protocole gRPC Proxy (Proxy 5.x)**, en couvrant toutes les capacités essentielles des Producer et Consumer ainsi que les fonctionnalités d'entreprise, avec des adaptateurs unifiés pour Alibaba Cloud, Huawei Cloud, Tencent Cloud et le système d'authentification Apache ACL.

**Principaux avantages** :

| Fonctionnalité | Description |
|---------|-------------|
| **Double protocole** | Remoting (mature et stable sur 4.x) + gRPC (tourné vers l'avenir sur 5.x), avec routage automatique |
| **Aucune dépendance externe** | Codec de champs Protobuf intégré (écrit à la main sur SpanWriter/SpanReader de NewLife.Core), aucune runtime Java ou gRPC requise |
| **Multi-cloud** | Interface unifiée `ICloudProvider` avec adaptateurs intégrés pour Alibaba Cloud, Huawei Cloud, Tencent Cloud et Apache ACL |
| **Prêt pour la production** | Fonctionnalités d'entreprise complètes : reprise de consommation, file de messages morts, vérification des transactions, consommation ordonnée, consommation Pop, etc. |
| **Couverture de frameworks la plus large** | De .NET Framework 4.5+ à .NET 10 ; les fonctionnalités gRPC sont disponibles à partir de .NET Standard 2.1+ |
| **Compatible AOT** | La cible net10.0 prend en charge NativeAOT/trimming : l'encodage/décodage des trames, la sérialisation JSON et la compression des messages ne reposent pas sur la réflexion d'exécution. Le dépôt contient un smoke test AOT ainsi qu'un smoke test AOT de consommation du paquet NuGet ; la CI valide la publication et l'exécution AOT sur une matrice de RID courants (win/linux/osx, musl et Loongson LA64) |
| **Localisation à l'exécution** | Les journaux, les exceptions d'analyse de protocole, les exceptions des API publiques et les descriptions de configuration utilisent l'anglais comme texte source, avec des listes de mots intégrées pour 31 langues (132 entrées), traduites automatiquement selon `Localization.Culture` avec repli sur l'anglais ; les journaux préfixés sont traduits avant la composition du préfixe, et le préfixe ne participe pas à la recherche dans la liste de mots |
| **Hautes performances** | Basé sur la couche réseau hautes performances NewLife.Net : réutilisation des connexions, canal VIP, compression des messages et contrôle de concurrence |

---

## Installation

```powershell
# Gestionnaire de packages NuGet
Install-Package NewLife.RocketMQ

# .NET CLI
dotnet add package NewLife.RocketMQ
```

```xml
<!-- PackageReference -->
<PackageReference Include="NewLife.RocketMQ" Version="3.0.*" />
```

---

## Démarrage rapide

### Envoi de messages

```csharp
using NewLife.RocketMQ;

var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    Group = "producer_group"
};
producer.Start();

// Envoi synchrone
var result = producer.Publish("Hello RocketMQ!");
Console.WriteLine($"Message ID: {result.MsgId}");

// Envoi asynchrone
await producer.PublishAsync("async message");

// Envoi par lots
await producer.PublishBatch(new[] { "message1", "message2", "message3" });
```

### Consommation de messages

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
    return true; // Retourner true indique que la consommation a réussi
};

consumer.Start();
```

### Accès gRPC (Proxy 5.x)

```csharp
// Producer : définir GrpcProxyAddress achemine le trafic par le chemin gRPC 5.x (nécessite netstandard2.1+)
var producer = new Producer
{
    Topic = "test_topic",
    Group = "producer_group",
    NameServerAddress = "127.0.0.1:9876",
    GrpcProxyAddress = "http://127.0.0.1:8081",
};
producer.Start();
await producer.PublishAsync("gRPC message");

// Consumer : les relations d'abonnement sont déclarées automatiquement au démarrage (connexion longue Telemetry) ; la réception et l'acquittement passent par gRPC
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
// ... traitement métier ...
await consumer.AckMessageViaGrpcAsync("test_topic",
[
    .. messages.Select(m => new AckMessageEntry
    {
        ReceiptHandle = m.SystemProperties?.ReceiptHandle,
        MessageId = m.SystemProperties?.MessageId,
    }),
]);
```

> La consommation en long polling peut aussi utiliser directement `GrpcPushConsumer` (sémantique PUSH_CONSUMER avec Ack et reprise automatiques intégrés).
>
> **Reprise POP et messages morts en 5.x** : les messages dont la consommation échoue sont écrits par le Broker dans `%RETRY%{group}_{topic}` puis, selon la `popFromRetryProbability`, réinjectés dans la réponse de tirage du topic d'origine ; le client n'a qu'à consommer le topic d'origine. Lorsque le nombre de remises atteint `GrpcPushConsumer.MaxDeliveryAttempts` (16 par défaut), le message est déplacé vers `%DLQ%{group}` ; dans les scénarios de réception manuelle, appelez `Consumer.ForwardToDeadLetterQueueViaGrpcAsync` (`GetGrpcDlqTopic()` renvoie le nom du topic des messages morts). La borne inférieure du temps invisible est de 10 secondes (ramenée automatiquement à cette valeur dans la bibliothèque) ; les valeurs plus petites sont rejetées par le Broker.
>
> Les messages morts doivent être traités en une seule passe : `dotnet run --file scripts/DlqTool.cs -- --group <groupe de consommateurs> --to <topic d'origine>` republie les messages de la file de messages morts vers le topic spécifié et n'acquitte que les messages republiés avec succès. Tirer des messages morts fait avancer l'offset de consommation, et les messages non acquittés sont déplacés par le Broker vers `%RETRY%{group}_%DLQ%{group}` ; la consultation en lecture seule de la file de messages morts n'est donc pas prise en charge.
>
> **Messages ordonnés FIFO et messages transactionnels** : le type du topic doit correspondre au type des messages ; créez-le avec `CreateTopic(topic, queueNum, 0, TopicMessageType.Fifo/Transaction)`. Pour les envois FIFO, définissez `Message.MessageGroup` ; côté consommateur, définissez `GrpcPushConsumer.Orderly = true` pour traiter en série et préserver l'ordre. Pour les messages transactionnels, utilisez `PublishTransactionViaGrpcAsync` pour envoyer le demi-message et `EndTransactionViaGrpcAsync` pour valider ou annuler (sans validation, le Broker l'annule après expiration du délai).

### Messages différés

```csharp
// 18 niveaux de délai prédéfinis
producer.PublishDelay("delayed message", DelayTimeLevels.s30);

// Le mode gRPC prend en charge les délais arbitraires (nécessite netstandard2.1+)
producer.GrpcProxyAddress = "http://127.0.0.1:8081";
await producer.PublishDelayViaGrpcAsync("arbitrary delay", DateTime.Now.AddMinutes(30));
```

### Messages transactionnels

```csharp
var producer = new Producer
{
    Topic = "tx_topic",
    Group = "tx_group",
    NameServerAddress = "127.0.0.1:9876"
};

// Callback de vérification des transactions
producer.OnCheckTransaction = (msg, transactionId) =>
{
    var success = CheckLocalTransaction(transactionId);
    return success ? TransactionState.Commit : TransactionState.Rollback;
};
producer.Start();

// Envoyer le demi-message -> exécuter la transaction locale -> valider/annuler
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

### Messages ordonnés

```csharp
// Les messages ayant la même clé vont dans la même file
var queue = producer.SelectQueue("order_123");
producer.Publish("ordered message 1", queue);
producer.Publish("ordered message 2", queue);

// Activer la consommation ordonnée côté consommateur
consumer.OrderConsume = true;
```

### Modèle requête-réponse

```csharp
// Le Producer envoie une requête (synchrone/asynchrone)
var response = producer.Request("request message", timeout: 5000);
var reply = await producer.RequestAsync("async request", timeout: 5000);

// Le Consumer répond
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

### Conversion de messages entre les deux protocoles

```csharp
// Convertir un message Remoting en message gRPC (nécessite netstandard2.1+)
var grpcMsg = message.ToGrpcMessage();

// Convertir un message gRPC en message Remoting étendu / message de base
var ext = grpcMsg.ToMessageExt();   // MessageExt (inclut toutes les propriétés système)
var msg = grpcMsg.ToMessage();      // Message (champs de base)
```

---

## Fonctionnalités avancées du Consumer

### Reprise de consommation et file de messages morts

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    EnableRetry = true,           // Activer la reprise de consommation
    MaxReconsumeTimes = 3         // Nombre maximal de reprises ; au-delà, les messages entrent dans la file %DLQ% des messages morts
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        try { ProcessMessage(msg); }
        catch { return false; } // Retourner false déclenche une reprise
    }
    return true;
};
```

### Filtrage par Tag / SQL92

```csharp
// Filtrage par tag
consumer.Tags = "TagA || TagB";

// Filtrage par expression SQL92
consumer.ExpressionType = "SQL92";
consumer.Subscription = "age > 18 AND city = 'Shanghai'";
```

### Abonnement multi-topic

```csharp
consumer.Topics = "topic1;topic2;topic3";
```

### Mode de consommation Pop

```csharp
// Consommation Pop (acquittement manuel)
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
        await consumer.ChangeInvisibleTimeAsync(msg, 30000); // Prolonger le temps de traitement
    }
}
```

### Limitation de consommation / canal VIP / compression des messages

```csharp
consumer.MaxConcurrentConsume = 10; // Traiter au plus 10 messages en parallèle

producer.VipChannelEnabled = true;  // Activer le canal VIP (BrokerPort - 2)
producer.CompressOverBytes = 4096;  // Compresser automatiquement en ZLIB les corps de message dépassant 4 Ko
```

### Passerelle HTTP (MQ -> WebAPI)

Pour transférer les messages consommés vers une WebAPI, ou exposer une WebAPI pour la publication externe de messages, vous pouvez combiner les API existantes ; voir le [guide de la passerelle HTTP](Doc/HttpBridge_Guide.md).

---

## Accès aux fournisseurs cloud

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
        InstanceId = "MQ_INST_xxx"  // Facultatif ; analysé automatiquement depuis l'adresse
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

### Authentification Apache RocketMQ ACL

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

## Publication NativeAOT

Seule la cible `net10.0` prend en charge NativeAOT/trimming (les frameworks cibles plus anciens n'ont pas la notion de trimming et se comportent à l'identique). Le paquet inclut déjà les accesseurs de membres générés à la compilation et les racines de métadonnées ; aucune configuration supplémentaire n'est donc nécessaire pour le consommer :

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

Scénarios vérifiés (mesurés avec des exécutables natifs) : encodage/décodage des trames, dictionnaires de propriétés d'en-tête de requête, sérialisation JSON, compression des messages (aller-retour ZLIB et limites de décompression), propriétés LMQ, signature ACL, adaptateurs de fournisseurs cloud, conversion entre les deux protocoles, et envoi/réception de bout en bout (corps normal/corps objet/lots/messages différés/compression des gros messages, consommation, interrogation d'offset, verrouillage ordonné, POP) ainsi que le chemin gRPC 5.x (routage, acquittement d'envoi/réception, consommation push, transfert vers les messages morts, consommation ordonnée FIFO).

Couverture CI : win-x64 (y compris l'intégralité de la chaîne locale RocketMQ), linux-x64, linux-arm64, win-arm64, osx-x64/osx-arm64, linux-musl-x64/arm64 sont chacun publiés nativement et exécutés ; Loongson linux-loongarch64 s'exécute via l'émulation en mode utilisateur QEMU plus le SDK Loongson (cette plateforme n'a actuellement pas de prise en charge officielle de NativeAOT, un smoke test JIT d'ingénierie de type AOT sert donc de critère).

**Les corps de type personnalisé doivent être enregistrés** : lorsqu'un type personnalisé (POCO) est utilisé comme corps de message, le trimmer supprime par défaut ses propriétés publiques, et le corps objet est silencieusement sérialisé en objet vide `{}`. Enregistrez une fois au démarrage de l'application chaque type utilisé comme corps de message :

```csharp
MqAot.RegisterBodyType<MyOrder>();
MqAot.RegisterBodyType<MyEvent>();
```

`MqAot.RegisterBodyType<T>()` n'a une sémantique de préservation au trimming que sur net10.0 ; c'est une opération sans effet sur les autres cibles, les projets multi-cibles peuvent donc partager le même code de démarrage. Vous pouvez aussi déclarer vos propres racines de métadonnées selon le même modèle que la bibliothèque (`[DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MyOrder))]` + `[ModuleInitializer]`).

---

## Aperçu de l'architecture

```
MqBase (classe de base métier)
├── Producer
└── Consumer

Couche de communication
├── Protocole Remoting (Broker 4.x/5.x)
│   ├── ClusterClient (connexion TCP longue, réutilisation de l'Opaque)
│   ├── NameClient (découverte de routes, interrogation toutes les 30 s)
│   └── BrokerClient (heartbeat/désenregistrement)
│
└── Protocole gRPC (Proxy 5.x, netstandard2.1+)
    ├── GrpcClient (HTTP/2, Unary + Server Streaming + connexion longue en écriture seule)
    ├── GrpcMessagingService (11 méthodes RPC)
    ├── GrpcTelemetrySession (connexion longue de remontée de ressources : renvoi périodique + reconnexion en cas de coupure)
    ├── MessageConverter (conversion de messages Remoting ↔ gRPC)
    └── Codec de champs (Protobuf écrit à la main sur SpanWriter/SpanReader)

Couche d'adaptation des fournisseurs cloud
├── AliyunProvider  (Alibaba Cloud : routage par ID d'instance + NameServer HTTP)
├── HuaweiProvider  (Huawei Cloud : SSL/TLS + routage par ID d'instance)
├── TencentProvider (Tencent Cloud : routage par préfixe Namespace)
└── AclProvider     (Apache ACL : signature HMAC-SHA1)
```

Voir le [document de besoins](Doc/需求文档.md), la [liste des fonctionnalités](Doc/功能清单.md), la [conception de l'architecture](Doc/架构设计.md), l'[analyse concurrentielle](Doc/竞品分析.md), le [guide de la passerelle HTTP](Doc/HttpBridge_Guide.md) et la [localisation](Doc/多语言本地化.md).

---

## Aperçu des fonctionnalités

### Producer

| Fonctionnalité | État | Description |
|---------|:------:|-------------|
| Envoi synchrone/asynchrone/one-way | ✅ | Publish / PublishAsync / PublishOneway |
| Envoi par lots | ✅ | PublishBatch, fusionnant plusieurs messages en une seule requête |
| Messages différés | ✅ | 18 niveaux prédéfinis + délais arbitraires via gRPC |
| Messages transactionnels | ✅ | Demi-message + commit/rollback + callback de vérification |
| Messages ordonnés | ✅ | Envoi vers un MessageQueue spécifié |
| Requête-réponse | ✅ | Requête-réponse synchrone/asynchrone |
| Compression des messages | ✅ | ZLIB automatique au-delà du seuil CompressOverBytes |
| Trace des messages | ✅ | AsyncTraceDispatcher + MessageTraceHook |

### Consumer

| Fonctionnalité | État | Description |
|---------|:------:|-------------|
| Consommation par tirage / ordonnancement | ✅ | Tirage en long polling avec attribution automatique des files |
| Consommation en cluster/diffusion | ✅ | Rééquilibrage par attribution moyenne / persistance locale des offsets |
| Filtrage par Tag / SQL92 | ✅ | Filtrage par expression |
| Abonnement multi-topic | ✅ | Propriété Topics ; rééquilibrage par topic |
| Reprise de consommation + file de messages morts | ✅ | EnableRetry + MaxReconsumeTimes |
| Consommation ordonnée | ✅ | Verrouillage de file (OrderConsume) |
| Consommation Pop | ✅ | Pop/Ack/BatchAck/ChangeInvisibleTime |
| Limitation de consommation | ✅ | Contrôle par sémaphore MaxConcurrentConsume |

### Gestion et exploitation

| Fonctionnalité | État | Description |
|---------|:------:|-------------|
| CRUD des topics/groupes de consommateurs | ✅ | Création/mise à jour/suppression |
| Interrogation des messages | ✅ | Par ID / par Key |
| Statistiques de consommation / informations de cluster | ✅ | GetConsumeStats / GetClusterInfo |
| Gestion et réinitialisation des offsets | ✅ | Consultation/mise à jour/réinitialisation |

### Protocoles et compatibilité

| Version du serveur | Remoting | gRPC | Description |
|----------------|:--------:|:----:|-------------|
| RocketMQ 4.0 ~ 4.9 | ✅ | — | Entièrement compatible |
| RocketMQ 5.x (Broker) | ✅ | — | Remoting reste rétrocompatible |
| RocketMQ 5.x (Proxy) | — | ✅ | Activé via GrpcProxyAddress |
| Alibaba Cloud 4.x | ✅ | — | Adaptateur AliyunProvider |
| Huawei Cloud DMS | ✅ | — | Adaptateur HuaweiProvider |
| Tencent Cloud TDMQ | ✅ | — | Adaptateur TencentProvider |

---

## Couverture des tests

La suite de tests xUnit couvre les fonctionnalités essentielles, les fonctionnalités avancées, la compatibilité de protocoles, les adaptateurs de fournisseurs cloud et les tests d'intégration E2E (les cas d'intégration gRPC sont ignorés automatiquement lorsqu'aucun Proxy n'est configuré) ; elle vérifie aussi la sémantique d'authentification d'Apache ACL 2.0 sur Remoting comme sur gRPC (les requêtes sans identifiants, provenant d'utilisateurs non administrateurs ou portant de mauvaises clés sont rejetées, tandis que les administrateurs peuvent envoyer et recevoir normalement). Les tests unitaires s'exécutent en parallèle par classe de test, et `dotnet test XUnitTestRocketMQ/XUnitTest.csproj --filter "Category!=Integration"` s'exécute en environ 4 secondes ; les cas d'intégration qui dépendent d'un Broker réel sont placés dans la collection RocketMQ et exécutés en série afin d'éviter les interférences dues aux topics et groupes de consommateurs partagés ; définissez `ROCKETMQ_TEST_VERBOSE=1` pour restaurer les journaux du framework lors du diagnostic. La CI s'exécute sous forme de matrice de RID : les tests unitaires et les smoke tests AOT couvrent win-x64 / linux-x64 / linux-arm64 / Loongson LA64 (émulation QEMU, obligatoire), tandis que win-arm64, les deux architectures osx et les deux architectures linux-musl sont en période d'observation ; tous peuvent être déclenchés manuellement.

Sont également inclus : les smoke tests NativeAOT (`AotSmoke`, couvrant la compression des messages, LMQ, la signature ACL, le chemin gRPC, le transfert vers la file de messages morts et la consommation ordonnée FIFO), les smoke tests AOT de consommation du paquet NuGet (`AotPackageSmoke`, consommant les artefacts packagés via PackageReference, couvrant les ressources du paquet, la sérialisation générée à la compilation, la compression et l'envoi/réception de bout en bout), la collecte de mesures de référence de publication (`scripts/AotBaseline.cs`, rapportant la taille / le surcoût de démarrage / le débit avec vérification de seuils et écriture dans le résumé du pipeline en CI), les benchmarks de comparaison de performances AOT/JIT (le projet `Benchmark` basé sur BenchmarkDotNet, incluant les statistiques d'allocation ; le côté AOT utilise un échantillonnage multi-tours via `AotSmoke --bench`), un outil d'exploitation de la file de messages morts (`scripts/DlqTool.cs`, republiant les messages de `%DLQ%{group}` vers un topic spécifié) et un script d'acceptation de stabilité gRPC (`scripts/GrpcStability.cs`, exécuté chaque nuit en CI avec un redémarrage de Proxy injecté).

### Tests d'intégration locaux (Windows)

```powershell
# Installation et démarrage en une étape de NameServer + Broker + Proxy (le stockage est vidé à chaque démarrage)
dotnet run --file scripts/RocketMqSetup.cs

$env:ROCKETMQ_NAMESERVER = "127.0.0.1:9876"
dotnet test XUnitTestRocketMQ\XUnitTest.csproj -c Release

# Arrêt / état
dotnet run --file scripts/RocketMqSetup.cs -- --stop
dotnet run --file scripts/RocketMqSetup.cs -- --status
```

Le démarrage local utilise par défaut un mode à faibles ressources : les journaux des trois JVM sont réduits à WARN (ils ne sont plus écrits dans `%USERPROFILE%\logs\rocketmqlogs`), l'intervalle de vidage asynchrone est de 5 secondes, le commitlog est de 64 Mo, les pools de threads sont resserrés, et le stockage est vidé à chaque démarrage afin que le Broker soit toujours dans un état neuf. Lors du diagnostic, utilisez `--log-level INFO` (ou `$env:ROCKETMQ_TEST_LOG_LEVEL="INFO"`) pour restaurer les journaux détaillés (le volume de journaux augmente sensiblement) ; lorsque gRPC n'est pas nécessaire, utilisez `--no-proxy` pour démarrer une JVM de moins ; pour placer le stockage sur un disque RAM, utilisez `--store-dir R:\RocketMQ\store`.

---

## Contribution

Les Issues et Pull Requests sont les bienvenues !

1. Forkez ce dépôt
2. Créez une branche de fonctionnalité (`git checkout -b feature/AmazingFeature`)
3. Commitez vos modifications (`git commit -m 'Add some AmazingFeature'`)
4. Poussez la branche (`git push origin feature/AmazingFeature`)
5. Ouvrez une Pull Request

---

## Licence

Ce projet est publié sous la [licence MIT](LICENSE).

---

## Carte de l'écosystème

NewLife est un écosystème en couches de projets open source versionnés indépendamment. La carte ci-dessous est reprise à l'identique par tous les dépôts NewLife ; chaque projet renvoie à son dépôt et à son paquet NuGet principal.

| Projet | Rôle | Téléchargements |
| --- | --- | --- |
| **Fondation** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | Journalisation, configuration, cache, réseau, sérialisation, ordonnancement et traçage APM. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | Intergiciel de données : tables uniques à l'échelle de dizaines de milliards, partitionnement automatique, séparation lecture/écriture et plus de 10 bases de données. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | Bibliothèque réseau hautes performances : 1,77 M pps / 14,5 Gbps sur une machine (TCP 1KB, 2026-10), 4 M connexions TCP persistantes. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | Communication protocolaire et framework RPC pour applications client/serveur et appareils IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | Installe des applications .NET comme démons système : services Windows et systemd Linux. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | Modèles de projet `dotnet new` pour console, web, API, service, données et IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **Données et messagerie** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Client Redis avec une latence en microsecondes, des millions d'opérations/s et des files de messages intégrées. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Client RocketMQ entièrement managé pour Apache RocketMQ et Alibaba Cloud Message Queue. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | Client et serveur MQTT, y compris la connectivité Alibaba Cloud IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | Base de données hybride en C# : embarquée ou en réseau, relationnelle, séries temporelles, file de messages et KV réunis. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | Pilote et couche d'assistance MySql construits sur la pile de données NewLife. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web, IA et distribué** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | Framework rapide de plateforme d'administration : permissions, SSO, serveur OAuth et échafaudage CRUD. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | Plateforme de services distribués : gestion des nœuds, APM, centre de configuration, registre et centre de publication. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | Plateforme de tâches distribuées et d'ordonnancement de données en temps réel avec partitionnement, reprises et backfill. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | Client unifié pour 46 fournisseurs de LLM, plus ChatAI, une application web de chat prête à l'emploi. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **Industriel et IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | Bibliothèque standard IoT définissant les contrats de protocole et les abstractions d'appareils. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | Implémentation Modbus TCP/RTU/ASCII au-dessus de NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | Protocole pour automate Siemens au-dessus de NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | Protocole pour automate Omron au-dessus de NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | Protocole pour automate Mitsubishi Melsec au-dessus de NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | Client OPC pour l'accès aux données industrielles. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | Pile de protocoles BACnet pour l'automatisation des bâtiments. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | Plateforme IoT open source légère avec gestion des passerelles et des appareils. | — |
| **Télématique embarquée** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | Protocole de positionnement de véhicules JT/T 808 : plus de 70 corps de message, 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | Protocole plateforme à plateforme JT/T 809 pour les autorités de transport. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | Vidéo embarquée JT/T 1078 : analyse RTP, remuxage FLV/fMP4 et diffusion en direct. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **Composants métier** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Un seul paquet pour Excel, Word, PPT, PDF et plus de 15 formats de documents. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | Audio multiplateforme : codecs, E/S d'appareils, DSP, conteneurs et streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | Codecs vidéo, formats de conteneurs, traitement des images et streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | Interface unifiée pour Baidu, AMap, Tencent et Tianditu avec géocodage et conversion de coordonnées. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | Géolocalisation IPv4 avec une base de données hors ligne mappée en mémoire et une API en ligne en secours. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **Outils et produits** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | Boîte à outils du développeur : réseau, port série, cryptographie, regex, Modbus et MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | Système de gestion de code source Git auto-hébergé. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | Installe automatiquement le runtime .NET correspondant lorsqu'il est lancé depuis un hôte plus ancien. | — |

> Les badges indiquent le paquet principal de chaque projet ; les paquets de thèmes, d'extensions et satellites sont répertoriés sur leurs pages NuGet. Les plus de 30 paquets non listés ici (FTP, LoRa, extensions Office, pilotes de protocoles, etc.) sont disponibles dans l'[organisation NewLifeX](https://github.com/NewLifeX) et sur [newlifex.com](https://newlifex.com).

## Équipe

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

NewLife a été fondée en 2002 et œuvre depuis plus de deux décennies dans l'écosystème .NET et IoT, maintient près de 100 projets open source (dont 34 principaux) et fournit des solutions matérielles et logicielles intégrées aux secteurs de l'énergie, de la logistique, de la santé, du contrôle industriel et d'Internet. Notre devise : « l'apprentissage ne connaît pas l'ancienneté — celui qui maîtrise devient maître ».

Les composants open source ont été téléchargés <!--nuget-total:start-->6.48M<!--nuget-total:end--> fois depuis NuGet (à l'échelle de l'écosystème, en date de 2026-10).

Site web : https://newlifex.com · Organisation open source : https://github.com/NewLifeX

<details>
<summary>Compte officiel WeChat : 智能大石头 (scannez pour suivre)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
