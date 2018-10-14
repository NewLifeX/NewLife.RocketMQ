namespace NewLife.RocketMQ.Protocol
{
    /// <summary>服务状态</summary>
    public enum ServiceState
    {
        CreateJust = 0,
        Running = 1,
        ShutdownAlready = 2,
        StartFailed = 3,
    }
}