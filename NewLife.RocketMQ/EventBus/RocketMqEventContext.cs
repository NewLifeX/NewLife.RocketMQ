using NewLife.Messaging;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ;

/// <summary>RocketMQ事件上下文</summary>
/// <remarks>
/// 在通用事件上下文基础上携带原始消息，便于订阅者读取 MsgId、Tags、Keys、ReconsumeTimes 等 MQ 元数据。
/// 原始消息由消费循环持有，仅在当前回调内有效，不得跨回调保存。
/// </remarks>
public class RocketMqEventContext : EventContext
{
    /// <summary>原始消息</summary>
    public MessageExt Message { get; set; }

    /// <summary>实例化RocketMQ事件上下文</summary>
    /// <param name="eventBus">事件总线</param>
    /// <param name="message">原始消息</param>
    public RocketMqEventContext(IEventBus eventBus, MessageExt message)
    {
        EventBus = eventBus;
        Message = message;
    }
}
