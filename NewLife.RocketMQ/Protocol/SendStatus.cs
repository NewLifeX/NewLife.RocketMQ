namespace NewLife.RocketMQ.Protocol
{
    /// <summary>发送状态</summary>
    public enum SendStatus
    {
        SendOK = 0,
        FlushDiskTimeout = 1,
        FlushSlaveTimeout = 2,
        SlaveNotAvailable = 3,
    }
}