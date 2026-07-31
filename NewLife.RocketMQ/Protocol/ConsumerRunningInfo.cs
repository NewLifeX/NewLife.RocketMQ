namespace NewLife.RocketMQ.Protocol;

/// <summary>消费者运行信息</summary>
class ConsumerRunningInfo
{
    #region 属性
    /// <summary>属性集合</summary>
    public IDictionary<String, String> Properties { get; set; }

    /// <summary>订阅集合</summary>
    public SubscriptionData[] SubscriptionSet { get; set; }

    /// <summary>队列表</summary>
    public String[] MqTable { get; set; }
    #endregion
}
