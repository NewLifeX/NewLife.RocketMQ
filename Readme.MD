# NewLife.RocketMQ - 企业级纯托管 RocketMQ 客户端

![GitHub top language](https://img.shields.io/github/languages/top/newlifex/newlife.rocketmq?logo=github)
![GitHub License](https://img.shields.io/github/license/newlifex/newlife.rocketmq?logo=github)
![Nuget Downloads](https://img.shields.io/nuget/dt/newlife.rocketmq?logo=nuget)
![Nuget](https://img.shields.io/nuget/v/newlife.rocketmq?logo=nuget)
![Nuget (with prereleases)](https://img.shields.io/nuget/vpre/newlife.rocketmq?label=dev%20nuget&logo=nuget)

**纯托管企业级 RocketMQ 客户端**，支持 `.NET Framework 4.5+` / `.NET Standard 2.0+` / `.NET Core` / `.NET 5+`。

**完全使用 C# 实现，零外部依赖（无需 Java、gRPC、Protobuf 第三方库）**

---

## 🎯 产品定位

NewLife.RocketMQ 是新生命团队开发的**企业级纯托管 RocketMQ 客户端**，专为 .NET 生态设计：

- ✅ **双协议支持**：同时支持 RocketMQ Remoting 协议（4.x）和 gRPC Proxy 协议（5.x）
- ✅ **零外部依赖**：纯 C# 实现，无需 Java 或 gRPC 运行时，内置轻量级 Protobuf 编解码器
- ✅ **多云适配**：统一接口适配阿里云、华为云、腾讯云及 Apache ACL 认证体系
- ✅ **生产就绪**：完整的消费重试、死信队列、事务回查、顺序消费等企业级特性
- ✅ **高性能**：VIP 通道、连接池、消息压缩、并发控制等性能优化手段

**支持 Apache RocketMQ 4.0+ 和 5.x 版本**

---

## 🚀 核心特性

### 生产者（Producer）
- ✅ 同步/异步/单向发送
- ✅ 批量消息发送
- ✅ 延迟消息（18级定时 + 任意时间延迟）
- ✅ 事务消息（半消息 + 事务回查）
- ✅ 顺序消息
- ✅ Request-Reply 模式
- ✅ 消息压缩（ZLIB）
- ✅ 消息轨迹追踪

### 消费者（Consumer）
- ✅ Pull 模式消费
- ✅ 集群消费 / 广播消费
- ✅ 消费者负载均衡（Rebalance）
- ✅ Tag 过滤 / SQL92 表达式过滤
- ✅ 多 Topic 订阅
- ✅ 消费重试 + 死信队列（DLQ）
- ✅ 顺序消费（队列锁定）
- ✅ Pop 消费模式
- ✅ 消费限流（并发控制）
- ✅ 按时间戳消费

### 管理功能
- ✅ Topic 创建/更新/删除
- ✅ 消费组创建/更新/删除
- ✅ 消息查询（按 ID / 按 Key）
- ✅ 消费统计查询
- ✅ 集群信息查询
- ✅ 偏移量管理与重置

### 云厂商适配
- ✅ 阿里云消息队列 RocketMQ（实例 ID 路由 + HTTP NameServer 发现）
- ✅ 华为云 DMS for RocketMQ（SSL/TLS + 实例 ID 路由）
- ✅ 腾讯云 TDMQ RocketMQ（Namespace 前缀路由）
- ✅ Apache RocketMQ ACL 认证（HMAC-SHA1 签名）

### 协议支持
- ✅ **Remoting 协议**（RocketMQ 4.x/5.x Broker）：成熟稳定，功能完整
- ✅ **gRPC Proxy 协议**（RocketMQ 5.x Proxy）：内置 Protobuf 编解码器，支持任意时间延迟消息

---

## 📦 安装

### NuGet 包管理器
```powershell
Install-Package NewLife.RocketMQ
```

### .NET CLI
```bash
dotnet add package NewLife.RocketMQ
```

### PackageReference
```xml
<PackageReference Include="NewLife.RocketMQ" Version="3.0.*" />
```

---

## 🔧 快速开始

### 1. 生产者发送消息

```csharp
using NewLife.RocketMQ;

var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    Group = "producer_group"
};

producer.Start();

// 同步发送
var result = producer.Publish("Hello RocketMQ!");
Console.WriteLine($"消息ID: {result.MsgId}");

// 异步发送
await producer.PublishAsync("异步消息");

// 批量发送
await producer.PublishBatch(new[] { "消息1", "消息2", "消息3" });
```

### 2. 消费者接收消息

```csharp
using NewLife.RocketMQ;

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
    return true; // 返回 true 表示消费成功
};

consumer.Start();
Console.WriteLine("按任意键退出...");
Console.ReadKey();
```

### 3. 延迟消息

```csharp
// 使用预设延迟等级（18级）
producer.PublishDelay("延迟消息", DelayTimeLevels.s30);

// gRPC 模式支持任意时间延迟
producer.GrpcProxyAddress = "http://127.0.0.1:8081";
await producer.PublishDelayViaGrpcAsync("任意延迟", DateTime.Now.AddMinutes(30));
```

### 4. 事务消息

```csharp
var producer = new Producer
{
    Topic = "tx_topic",
    Group = "tx_group",
    NameServerAddress = "127.0.0.1:9876"
};

// 设置事务回查回调
producer.OnCheckTransaction = (msg, transactionId) =>
{
    // 根据 transactionId 查询本地事务状态
    var success = CheckLocalTransaction(transactionId);
    return success ? TransactionState.Commit : TransactionState.Rollback;
};

producer.Start();

// 发送半消息
var sendResult = producer.PublishTransaction("订单创建");

try
{
    // 执行本地事务
    ExecuteLocalTransaction(sendResult.TransactionId);
    
    // 提交事务
    producer.EndTransaction(sendResult, TransactionState.Commit);
}
catch
{
    // 回滚事务
    producer.EndTransaction(sendResult, TransactionState.Rollback);
}
```

### 5. Request-Reply 模式

**生产者端：**
```csharp
var producer = new Producer
{
    Topic = "request_topic",
    NameServerAddress = "127.0.0.1:9876"
};
producer.Start();

// 同步请求（等待响应）
var response = producer.Request("请求消息", timeout: 5000);
Console.WriteLine($"收到响应: {response.BodyString}");

// 异步请求
var reply = await producer.RequestAsync("异步请求", timeout: 5000);
```

**消费者端：**
```csharp
var consumer = new Consumer
{
    Topic = "request_topic",
    Group = "request_group",
    NameServerAddress = "127.0.0.1:9876"
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        if (!String.IsNullOrEmpty(msg.CorrelationId))
        {
            // 处理请求并发送响应
            var result = ProcessRequest(msg.BodyString);
            consumer.SendReply(msg, result);
        }
    }
    return true;
};

consumer.Start();
```

### 6. 顺序消息

```csharp
// 发送顺序消息（相同 key 的消息进入同一队列）
var queue = producer.SelectQueue("order_123");
producer.Publish("顺序消息1", queue);
producer.Publish("顺序消息2", queue);

// 消费端启用顺序消费
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    OrderConsume = true  // 启用顺序消费
};
```

---

## 🌩️ 云厂商接入

### 阿里云消息队列 RocketMQ

```csharp
using NewLife.RocketMQ;

var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "http://MQ_INST_xxx.aliyuncs.com:80",
    CloudProvider = new AliyunProvider
    {
        AccessKey = "你的AccessKey",
        SecretKey = "你的SecretKey",
        InstanceId = "MQ_INST_xxx"  // 可选，自动从地址解析
    }
};
producer.Start();
```

### 华为云 DMS for RocketMQ

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
        EnableSsl = true  // 启用SSL加密
    }
};
producer.Start();
```

### 腾讯云 TDMQ RocketMQ

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
producer.Start();
```

### Apache RocketMQ ACL 认证

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
producer.Start();
```

---

## 🎓 进阶使用

### 消费重试与死信队列

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    EnableRetry = true,           // 启用消费重试
    MaxReconsumeTimes = 3         // 最大重试次数
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        try
        {
            ProcessMessage(msg);
        }
        catch
        {
            // 返回 false 触发重试
            return false;
        }
    }
    return true;
};

// 超过最大重试次数后，消息自动进入死信队列：%DLQ%{ConsumerGroup}
```

### Tag 过滤

```csharp
// 生产者发送带 Tag 的消息
producer.Publish("消息内容", tag: "TagA");

// 消费者订阅指定 Tag
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    Tags = "TagA || TagB",  // 订阅 TagA 或 TagB
    NameServerAddress = "127.0.0.1:9876"
};
```

### SQL92 表达式过滤

```csharp
// 生产者发送带自定义属性的消息
var msg = new Message
{
    BodyString = "消息内容",
    ["age"] = "25",
    ["city"] = "Shanghai"
};
producer.Publish(msg);

// 消费者使用 SQL92 表达式过滤
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    ExpressionType = "SQL92",
    Subscription = "age > 18 AND city = 'Shanghai'",
    NameServerAddress = "127.0.0.1:9876"
};
```

### 多 Topic 订阅

```csharp
var consumer = new Consumer
{
    Topics = "topic1;topic2;topic3",  // 订阅多个 Topic
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876"
};

consumer.OnConsume = (q, messages) =>
{
    foreach (var msg in messages)
    {
        Console.WriteLine($"Topic: {msg.Topic}, 消息: {msg.BodyString}");
    }
    return true;
};
```

### Pop 消费模式

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876"
};

consumer.Start();

// Pop 消费（不自动提交偏移）
var messages = await consumer.PopMessageAsync(timeout: 10000);
foreach (var msg in messages)
{
    try
    {
        ProcessMessage(msg);
        // 手动确认
        await consumer.AckMessageAsync(msg);
    }
    catch
    {
        // 修改不可见时间（延长处理时间）
        await consumer.ChangeInvisibleTimeAsync(msg, 30000);
    }
}
```

### VIP 通道

```csharp
var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    VipChannelEnabled = true  // 启用 VIP 通道（使用 BrokerPort - 2）
};
```

### 消息压缩

```csharp
var producer = new Producer
{
    Topic = "test_topic",
    NameServerAddress = "127.0.0.1:9876",
    CompressOverBytes = 4096  // 消息体超过 4KB 自动 ZLIB 压缩
};
```

### 消费限流

```csharp
var consumer = new Consumer
{
    Topic = "test_topic",
    Group = "consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    MaxConcurrentConsume = 10  // 最多同时处理 10 条消息
};
```

---

## 📚 文档

- [架构设计文档](Doc/架构设计.md)
- [功能分析与兼容性报告](Doc/NewLife.RocketMQ功能分析与兼容性报告.md)
- [Request-Reply 使用指南](Doc/RequestReply_Guide.md)
- [在线教程](https://newlifex.com/core/rocketmq)

---

## 🏗️ 架构特点

### 双协议架构

```
MqBase (业务基类)
├── Producer (生产者)
└── Consumer (消费者)

通信层
├── Remoting 协议（4.x/5.x Broker）
│   ├── ClusterClient (TCP 长连接)
│   ├── NameClient (路由发现)
│   └── BrokerClient (心跳/注销)
│
└── gRPC 协议（5.x Proxy，netstandard2.1+）
    ├── GrpcClient (HTTP/2 客户端)
    ├── GrpcMessagingService (11 个 RPC 方法)
    └── ProtoWriter/ProtoReader (自研 Protobuf 编解码器)
```

### 云厂商适配层

```
ICloudProvider (统一云厂商接口)
├── AliyunProvider (阿里云)
├── HuaweiProvider (华为云)
├── TencentProvider (腾讯云)
└── AclProvider (Apache ACL)
```

---

## 🔬 测试覆盖

30+ 测试类覆盖核心功能：

- ✅ **核心功能**：ProducerTests、ConsumerTests、CommandTests、MessageTests
- ✅ **高级特性**：TransactionCheckTests、BatchMessageTests、RetryTests、OrderConsumeTests
- ✅ **协议兼容**：IPv6Tests、MessageId5xTests、MQVersionTests、ProtoTests
- ✅ **云厂商**：AliyunTests、CloudProviderTests
- ✅ **性能优化**：CompressionTests、ConcurrentConsumeTests、VipChannelTests

---

## 📊 性能优势

| 特性 | NewLife.RocketMQ | 官方 Java 客户端 |
|------|:----------------:|:---------------:|
| 部署复杂度 | ✅ 单一 DLL，零依赖 | ⚠️ 需要 JRE 环境 |
| 跨平台 | ✅ .NET Framework 4.5+ ~ .NET 10 | ✅ 需要对应平台 JRE |
| 功能完整度 | ✅ 4.x 核心功能 100%，5.x 90% | ✅ 100% |
| 性能 | ✅ 高性能（.NET 原生优化） | ✅ 高性能 |
| gRPC 支持 | ✅ 自研编解码器，零依赖 | ⚠️ 依赖 gRPC 库 |

---

## 🤝 参与贡献

欢迎提交 Issue 和 Pull Request！

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 提交 Pull Request

---

## 📄 许可协议

本项目采用 [MIT License](LICENSE) 开源协议。

---

## 新生命项目矩阵

各项目默认支持 net10.0/net9.0/netstandard2.1/netstandard2.0/net4.62/net4.5

|                               项目                               | 年份  | 说明                                                                                        |
| :--------------------------------------------------------------: | :---: | ------------------------------------------------------------------------------------------- |
|                             基础组件                             |       | 支撑其它中间件以及产品项目                                                                  |
|          [NewLife.Core](https://github.com/NewLifeX/X)           | 2002  | 核心库，日志、配置、缓存、网络、序列化、APM性能追踪                                         |
|    [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode)    | 2005  | 大数据中间件，单表百亿级，MySql/SQLite/SqlServer/Oracle/PostgreSql/达梦，自动分表，读写分离 |
|      [NewLife.Net](https://github.com/NewLifeX/NewLife.Net)      | 2005  | 网络库，单机千万级吞吐率（2266万tps），单机百万级连接（400万Tcp长连接）                     |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | 2011  | 协议通信库，提供CS应用通信框架，支持Http/RPC通信框架，高吞吐，物联网设备低开销易接入        |
|     [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube)     | 2010  | 魔方快速开发平台，集成了用户权限、SSO登录、OAuth服务端等，单表100亿级项目验证               |
|    [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent)    | 2008  | 服务管理组件，把应用安装成为操作系统守护进程，Windows服务、Linux的Systemd                   |
|     [NewLife.Zero](https://github.com/NewLifeX/NewLife.Zero)     | 2020  | Zero零代脚手架，基于NewLife组件生态的项目模板NewLife.Templates，Web、WebApi、Service        |
|                              中间件                              |       | 对接知名中间件平台                                                                          |
|    [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis)    | 2017  | Redis客户端，微秒级延迟，百万级吞吐，丰富的消息队列，百亿级数据量项目验证                   |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | 2018  | RocketMQ纯托管客户端，支持Apache RocketMQ和阿里云消息队列，十亿级项目验证                   |
|     [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT)     | 2019  | 物联网消息协议，MqttClient/MqttServer，客户端支持阿里云物联网                               |
|      [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT)      | 2022  | IoT标准库，定义物联网领域的各种通信协议标准规范                                             |
|   [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus)   | 2022  | ModbusTcp/ModbusRTU/ModbusASCII，基于IoT标准库实现，支持ZeroIoT平台和IoTEdge网关            |
|  [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens)  | 2022  | 西门子PLC协议，基于IoT标准库实现，支持IoT平台和IoTEdge                                      |
|      [NewLife.Map](https://github.com/NewLifeX/NewLife.Map)      | 2022  | 地图组件库，封装百度地图、高德地图、腾讯地图、天地图                                        |
|    [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio)    | 2023  | 音频编解码库，PCM/ADPCMA/G711A/G722U/WAV/AAC                                                |
|                             产品平台                             |       | 产品平台级，编译部署即用，个性化自定义                                                      |
|         [Stardust](https://github.com/NewLifeX/Stardust)         | 2018  | 星尘，分布式服务平台，节点管理、APM监控中心、配置中心、注册中心、发布中心                   |
|           [AntJob](https://github.com/NewLifeX/AntJob)           | 2019  | 蚂蚁调度，分布式大数据计算平台（实时/离线），蚂蚁搬家分片思想，万亿级数据量项目验证         |
|      [NewLife.ERP](https://github.com/NewLifeX/NewLife.ERP)      | 2021  | 企业ERP，产品管理、客户管理、销售管理、供应商管理                                           |
|         [CrazyCoder](https://github.com/NewLifeX/XCoder)         | 2006  | 码神工具，众多开发者工具，网络、串口、加解密、正则表达式、Modbus、MQTT                      |
|           [EasyIO](https://github.com/NewLifeX/EasyIO)           | 2023  | 简易文件存储，支持分布式系统中文件集中存储                                                  |
|           [XProxy](https://github.com/NewLifeX/XProxy)           | 2005  | 产品级反向代理，NAT代理、Http代理                                                           |
|        [HttpMeter](https://github.com/NewLifeX/HttpMeter)        | 2022  | Http压力测试工具                                                                            |
|         [GitCandy](https://github.com/NewLifeX/GitCandy)         | 2015  | Git源代码管理系统                                                                           |
|          [SmartOS](https://github.com/NewLifeX/SmartOS)          | 2014  | 嵌入式操作系统，完全独立自主，支持ARM Cortex-M芯片架构                                      |
|          [SmartA2](https://github.com/NewLifeX/SmartA2)          | 2019  | 嵌入式工业计算机，物联网边缘网关，高性能.NET8主机，应用于工业、农业、交通、医疗             |
|                          FIoT物联网平台                          | 2020  | 物联网整体解决方案，建筑、环保、农业，软硬件及大数据分析一体化，单机十万级点位项目验证      |
|                        UWB高精度室内定位                         | 2020  | 厘米级（10~20cm）高精度室内定位，软硬件一体化，与其它系统联动，大型展厅项目验证             |

---

## 新生命开发团队

![XCode](https://newlifex.com/logo.png)  

新生命团队（NewLife）成立于2002年，是新时代物联网行业解决方案提供者，致力于提供软硬件应用方案咨询、系统架构规划与开发服务。  

团队主导的80多个开源项目已被广泛应用于各行业，Nuget累计下载量高达**400余万次**。  

团队开发的大数据中间件 **NewLife.XCode**、蚂蚁调度计算平台 **AntJob**、星尘分布式平台 **Stardust**、缓存队列组件 **NewLife.Redis** 以及物联网平台 **FIoT**，均成功应用于电力、高校、互联网、电信、交通、物流、工控、医疗、文博等行业，为客户提供了大量先进、可靠、安全、高质量、易扩展的产品和系统集成服务。  

我们将不断通过服务的持续改进，成为客户长期信赖的合作伙伴，通过不断的创新和发展，成为国内优秀的 IoT 服务供应商。  

**新生命团队始于2002年，部分开源项目具有20年以上漫长历史，源码库保留有2010年以来所有修改记录**  

- 网站：<https://newlifex.com>  
- 开源：<https://github.com/newlifex>  
- QQ群：1600800 / 1600838  
- 微信公众号：

![智能大石头](https://newlifex.com/stone.jpg)