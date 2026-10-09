namespace NewLife.RocketMQ.Protocol;

/// <summary>结束事务请求头</summary>
public class EndTransactionRequestHeader
{
    #region 属性
    /// <summary>生产组</summary>
    public String ProducerGroup { get; set; }

    /// <summary>事务状态表偏移</summary>
    public Int64 TranStateTableOffset { get; set; }

    /// <summary>提交日志偏移</summary>
    public Int64 CommitLogOffset { get; set; }

    /// <summary>提交或回滚标记</summary>
    public Int32 CommitOrRollback { get; set; }

    /// <summary>是否来自事务回查</summary>
    public Boolean FromTransactionCheck { get; set; }

    /// <summary>消息编号</summary>
    public String MsgId { get; set; }

    /// <summary>事务编号</summary>
    public String TransactionId { get; set; }
    #endregion

    #region 方法
    /// <summary>获取属性字典。显式列出属性，避免反射在 NativeAOT 裁剪后失效</summary>
    /// <returns></returns>
    public IDictionary<String, Object> GetProperties() => new Dictionary<String, Object>
    {
        ["producerGroup"] = ProducerGroup,
        ["tranStateTableOffset"] = TranStateTableOffset,
        ["commitLogOffset"] = CommitLogOffset,
        ["commitOrRollback"] = CommitOrRollback,
        ["fromTransactionCheck"] = FromTransactionCheck,
        ["msgId"] = MsgId,
        ["transactionId"] = TransactionId,
    };
    #endregion
}
