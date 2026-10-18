# RocketMQ HTTP 桥接使用指南

## 概述

NewLife.RocketMQ 是纯托管客户端库，**不内置网关进程**（不引入 ASP.NET Core、Redis 等外部依赖）。实际项目中的“HTTP 桥接”有两种方向，都可以用现有 API 在应用侧组合实现，库侧无需改动：

| 方向 | 模式 | 组成 |
|------|------|------|
| MQ → WebAPI | Sink | `Consumer.OnConsumeAsync` + `HttpClient` |
| WebAPI → MQ | Source | ASP.NET Core Controller + `Producer.PublishAsync` |

> 不建议在链路上额外加 Redis 等中间层：RocketMQ 本身支持多个消费组各自全量消费，多一跳只增加延迟、一致性与运维成本。只有“消费方只能访问 Redis、无法直连 Broker”这类硬约束，才值得引入中间层。

---

## 一、MQ → WebAPI（Sink）

### 1.1 基本写法

```csharp
using System.Net.Http;
using System.Net.Http.Headers;
using NewLife.RocketMQ;

using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };

using var consumer = new Consumer
{
    Topic = "order_topic",
    Group = "http_sink_group",
    NameServerAddress = "127.0.0.1:9876",
    EnableRetry = true,        // 默认 true，消费失败自动发回 RETRY Topic
    MaxReconsumeTimes = 16,    // 超过后进入 %DLQ% 死信队列
    MaxConcurrentConsume = 10, // 需要限流时设置，0 表示不限
};

consumer.OnConsumeAsync = async (queue, messages, token) =>
{
    foreach (var msg in messages)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://api.example.com/hooks/order")
        {
            // 原样转发消息体，避免二次序列化改变下游格式
            Content = new ByteArrayContent(msg.Body ?? new Byte[0]),
        };
        request.Content.Headers.ContentType = new("application/json");
        request.Headers.Add("X-MQ-MsgId", msg.MsgId);   // 下游据此做幂等

        using var response = await http.SendAsync(request, token);
        if (!response.IsSuccessStatusCode) return false; // 整批回退重试
    }

    return true;
};

consumer.Start();
```

消息体是文本/JSON 时，也可以用 `msg.BodyString` 直接构造 `StringContent`；二进制消息请保留 `ByteArrayContent`，不要转字符串。

### 1.2 失败语义（重要）

`OnConsume` / `OnConsumeAsync` 返回 `false` 时，Consumer 会对**本批全部消息**调用 `SendMessageBack`，发回 Broker 的 RETRY Topic；`ReconsumeTimes` 达到 `MaxReconsumeTimes` 后进入 `%DLQ%`。

由此带来两个必须注意的点：

- **整批回退**：某一条 POST 失败会导致同批已成功的消息被再次投递，**下游必须按 `MsgId` 做幂等**。
- **重试计数**：`MessageExt.ReconsumeTimes` 可透传给下游，超过阈值后的消息应监控 `%DLQ%`，不要静默堆积。

### 1.3 元数据映射建议

| RocketMQ | HTTP 位置 | 说明 |
|----------|-----------|------|
| `MsgId` | 请求头 `X-MQ-MsgId` | 幂等键，取值安全 |
| `Topic` | 请求头或路径 | 一个 Sink 服务多个 Topic 时使用 |
| `Body` | 请求体 | 原样转发 |
| `Tags` / `Keys` | 自定义头或查询参数 | 头值不得包含 CR/LF，`Keys` 建议放请求体或查询参数 |
| `ReconsumeTimes` | 自定义头 | 便于下游识别重试消息 |

### 1.4 可靠性清单

- **超时**：`HttpClient.Timeout` 必须设置，避免消费线程被长时间占用；重试由 Broker 负责，不要在委托内做无界重试。
- **异常不外泄**：委托内捕获异常并返回 `false`，异常逃逸会中断本次消费调度。
- **不要吞消息**：POST 失败却返回 `true` 等于丢消息。
- **吞吐**：逐条串行 POST 的吞吐远低于消费能力，需要时对单批消息并发投递（`Task.WhenAll` + 信号量），或改走下游的批量接口。
- **顺序**：集群消费下多队列并行处理，天然不保证全局顺序；需要顺序语义时设置 `consumer.OrderConsume = true`（顺序消费），并按 `queue` 维度串行投递。
- **日志与追踪**：失败时记录 `MsgId`、HTTP 状态码与目标地址，便于对账（可用 `Tracer.NewSpan` 埋点）。

---

## 二、WebAPI → MQ（Source）

### 2.1 生产者注册（单例）

Producer 内部管理连接与路由，启动一次即可反复发送，随宿主释放：

```csharp
using NewLife.RocketMQ;

builder.Services.AddSingleton(_ =>
{
    var producer = new Producer
    {
        Topic = "order_topic",
        Group = "http_source_group",
        NameServerAddress = "127.0.0.1:9876",
    };
    producer.Start();
    return producer;
});
```

### 2.2 发布接口

```csharp
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;

[ApiController]
[Route("api/mq")]
public class MqPublishController : ControllerBase
{
    private static readonly HashSet<String> _topics = new(StringComparer.OrdinalIgnoreCase) { "order_topic" };

    private readonly Producer _producer;
    public MqPublishController(Producer producer) => _producer = producer;

    [Authorize]                       // 发布接口等于 MQ 写权限，必须鉴权
    [HttpPost("{topic}")]
    public async Task<IActionResult> Publish(String topic, [FromBody] PublishRequest request, CancellationToken token)
    {
        if (!_topics.Contains(topic)) return Forbid();   // Topic 白名单

        var msg = new Message
        {
            Topic = topic,
            BodyString = request.Body,
            Tags = request.Tags,
            Keys = request.Keys,
        };

        var rs = await _producer.PublishAsync(msg, null, token);
        return rs.Status == SendStatus.SendOK
            ? Ok(new { rs.MsgId })
            : StatusCode(500, new { rs.Status });
    }
}

public class PublishRequest
{
    public String Body { get; set; }
    public String Tags { get; set; }
    public String Keys { get; set; }
}
```

### 2.3 返回状态说明

`SendStatus` 取值：`SendOK`、`FlushDiskTimeout`、`FlushSlaveTimeout`、`SlaveNotAvailable`。非 `SendOK` **不等于一定没投递**（例如刷盘超时可能已写入 CommitLog），调用方重试前需按业务键做幂等。

---

## 三、为什么不在本库内置网关

- **定位**：本库是纯托管客户端，覆盖 `net45` ~ `net10`，只依赖 NewLife.Core，零外部依赖；网关需要 Web 宿主、鉴权、部署与运维，属于应用层而非协议层。
- **实现成本低**：上面的 Sink 与 Source 各约 20~40 行代码，使用方按自己的消息格式、鉴权与重试策略实现更合适。
- **需要开箱即用网关时**：应另建独立项目（Source / Sink 连接器），参考 Apache [rocketmq-connect](https://github.com/apache/rocketmq-connect) 的 HTTP Sink 连接器；Redis 中间层仅在确认存在“消费方无法直连 Broker”的约束后再引入。

---

## 四、常见问题

**Q：消息体是二进制怎么办？**

用 `ByteArrayContent` 原样转发，不要把 `Body` 转成字符串。

**Q：能不能同步拿到下游处理结果再返回给生产者？**

可以，使用 Request-Reply 模式，见 [RequestReply 使用指南](RequestReply_Guide.md)。

**Q：Sink 多实例部署会不会重复消费？**

同一 `Group` 下由 Broker 分配队列，正常情况下不会并发重复消费（失败重试与 Rebalance 期间仍可能出现重复，因此仍需按 `MsgId` 幂等）；不同 `Group` 则各自全量消费，按业务需要选择。

---

（完）
