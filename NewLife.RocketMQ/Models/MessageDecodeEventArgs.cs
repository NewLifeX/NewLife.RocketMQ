using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ.Models;

/// <summary>消息解码失败事件参数</summary>
public class MessageDecodeEventArgs : EventArgs
{
    /// <summary>队列</summary>
    public MessageQueue Queue { get; set; }

    /// <summary>解码失败信息，含异常与原始消息字节</summary>
    public MessageDecodeError Error { get; set; }
}
