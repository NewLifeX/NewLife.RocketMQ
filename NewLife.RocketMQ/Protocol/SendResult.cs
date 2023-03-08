namespace NewLife.RocketMQ.Protocol;

/// <summary>发送状态</summary>
public enum SendStatus
{
    /// <summary>成功</summary>
    SendOK = 0,

    /// <summary>刷盘超时</summary>
    FlushDiskTimeout = 1,

    /// <summary>刷从机超时</summary>
    FlushSlaveTimeout = 2,

    /// <summary>从机不可用</summary>
    SlaveNotAvailable = 3,

    /// <summary>发送失败</summary>
    SendError = 4,
}

/// <summary>发送结果</summary>
public class SendResult
{
    #region 属性
    /// <summary>头部</summary>
    public Header Header { get; set; }

    /// <summary>状态</summary>
    public SendStatus Status { get; set; }

    /// <summary>消息编号</summary>
    public String MsgId { get; set; }

    /// <summary>队列</summary>
    public MessageQueue Queue { get; set; }

    /// <summary>队列偏移</summary>
    public Int64 QueueOffset { get; set; }

    /// <summary>事务编号</summary>
    public String TransactionId { get; set; }

    /// <summary>偏移消息编号</summary>
    public String OffsetMsgId { get; set; }

    /// <summary>区域</summary>
    public String RegionId { get; set; }
    #endregion

    #region 方法
    /// <summary>读取结果</summary>
    /// <param name="dic"></param>
    public void Read(IDictionary<String, String> dic)
    {
        if (dic == null) return;

        var dic2 = dic.ToNullable(StringComparer.OrdinalIgnoreCase);

        if (dic2.TryGetValue(nameof(MsgId), out var str)) MsgId = str;
        if (dic2.TryGetValue(nameof(OffsetMsgId), out str)) OffsetMsgId = str;
        if (dic2.TryGetValue(nameof(QueueOffset), out str)) QueueOffset = str.ToLong();
        if (dic2.TryGetValue(nameof(TransactionId), out str)) TransactionId = str;
        if (dic2.TryGetValue(nameof(RegionId), out str)) RegionId = str;
        if (dic2.TryGetValue("MSG_REGION", out str)) RegionId = str;
    }

    /// <summary>
    /// 已重载。友好显示
    /// </summary>
    /// <returns></returns>
    public override String ToString() => $"SendStatus={Status} MsgId={MsgId} OffsetMsgId={OffsetMsgId} QueueOffset={QueueOffset} Queue={Queue}";
    #endregion
}