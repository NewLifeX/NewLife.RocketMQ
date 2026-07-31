namespace NewLife.RocketMQ.Models;

/// <summary>首次消费时的起始位置。与 RocketMQ 协议字符串一一对应</summary>
public enum ConsumeFromWheres
{
    /// <summary>从最新位置开始</summary>
    CONSUME_FROM_LAST_OFFSET = 0,

    /// <summary>从最早位置开始</summary>
    CONSUME_FROM_FIRST_OFFSET = 1,

    /// <summary>从指定时间戳开始</summary>
    CONSUME_FROM_TIMESTAMP = 2,

    /// <summary>从最小偏移开始</summary>
    CONSUME_FROM_MIN_OFFSET = 3,

    /// <summary>从最大偏移开始</summary>
    CONSUME_FROM_MAX_OFFSET = 4,
}