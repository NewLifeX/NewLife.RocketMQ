namespace NewLife.RocketMQ.Protocol;

/// <summary>Topic 消息类型。用于 <see cref="NewLife.RocketMQ.MqBase.CreateTopic"/> 的 topicMessageType 请求头字段。</summary>
/// <remarks>
/// 对应 RocketMQ Broker 端的 <c>TopicMessageType</c> 枚举。
/// LITE 类型需要 RocketMQ 5.5.0+ Broker（RIP-83），旧版 Broker 会忽略该字段并以 Normal 处理。
/// </remarks>
public enum TopicMessageType
{
    /// <summary>普通消息（默认）</summary>
    Normal = 0,

    /// <summary>有序消息（FIFO）</summary>
    Fifo = 1,

    /// <summary>延迟消息</summary>
    Delay = 2,

    /// <summary>事务消息</summary>
    Transaction = 3,

    /// <summary>Lite Topic（轻量动态 Topic，RocketMQ 5.5.0+ RIP-83）。
    /// 支持按需动态创建数百万级轻量 Topic，适合 AI Agent 等海量临时 Topic 场景，无需提前运维创建。
    /// 注意：仅支持 RocketMQ 5.5.0+ Broker，旧版 Broker 会忽略该字段。</summary>
    Lite = 4,
}
