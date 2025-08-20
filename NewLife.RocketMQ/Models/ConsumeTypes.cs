namespace NewLife.RocketMQ.Models;

/// <summary>消费类型</summary>
public enum ConsumeTypes
{
    /// <summary>拉取。</summary>
    Pull,

    /// <summary>推送。</summary>
    Push,

    /// <summary>弹出。</summary>
    Pop,
}
