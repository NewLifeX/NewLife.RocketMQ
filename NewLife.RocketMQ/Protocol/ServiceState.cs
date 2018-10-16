namespace NewLife.RocketMQ.Protocol
{
    /// <summary>服务状态</summary>
    public enum ServiceState
    {
        /// <summary>刚刚建立</summary>
        CreateJust = 0,

        /// <summary>运行中</summary>
        Running = 1,

        /// <summary>已经关闭</summary>
        ShutdownAlready = 2,

        /// <summary>启动失败</summary>
        StartFailed = 3,
    }
}