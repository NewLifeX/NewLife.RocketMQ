namespace NewLife.RocketMQ.Protocol.ConsumerStates;

/// <summary>
/// 消息队列信息模型
/// </summary>
public class MessageQueueModel
{
    /// <summary>
    /// Broker服务器名称
    /// </summary>
    public String BrokerName { get; set; }

    /// <summary>
    /// 队列编码
    /// </summary>
    public Int32 QueueId { get; set; }

    /// <summary>
    /// 主题
    /// </summary>
    public String Topic { get; set; }

    /// <summary>
    /// 阿里版本返回字段
    /// </summary>
    public Boolean MainQueue { get; set; }

    /// <summary>
    /// 阿里版本返回字段
    /// </summary>
    public Int32 QueueGroupId { get; set; }

    /// <summary>已重载。</summary>
    /// <returns></returns>
    public override String ToString() => $"{BrokerName}[{QueueId}]";   
}
