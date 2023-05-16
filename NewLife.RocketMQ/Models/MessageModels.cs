namespace NewLife.RocketMQ.Models;

/// <summary>消息模型。广播/集群</summary>
public enum MessageModels
{
    /// <summary>集群。消费组内各消费者分享数据</summary>
    Clustering,

    /// <summary>广播。消费组内各消费者各自消费全部</summary>
    Broadcasting,
}