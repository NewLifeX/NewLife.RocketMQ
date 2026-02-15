namespace NewLife.RocketMQ.Protocol;

/// <summary>事务状态</summary>
public enum TransactionState
{
    /// <summary>预备事务（半消息）</summary>
    Prepared = 4,

    /// <summary>提交事务</summary>
    Commit = 8,

    /// <summary>回滚事务</summary>
    Rollback = 12,
}
