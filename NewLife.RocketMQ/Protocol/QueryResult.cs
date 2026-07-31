namespace NewLife.RocketMQ.Protocol;

/// <summary>查询结果</summary>
public class QueryResult
{
    /// <summary>最后更新时间。毫秒时间戳，需 Int64（Int32 会溢出）</summary>
    public Int64 IndexLastUpdateTimestamp { get; set; }

    /// <summary>消息列表</summary>
    public List<MessageExt> MessageList { get; set; }
}