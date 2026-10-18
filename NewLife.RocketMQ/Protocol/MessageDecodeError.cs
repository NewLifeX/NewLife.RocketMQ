namespace NewLife.RocketMQ.Protocol;

/// <summary>消息解码失败信息</summary>
/// <remarks>
/// 拉取批中单条消息无法解码时产生，携带原始字节，供应用层告警或旁路保存。
/// </remarks>
public class MessageDecodeError
{
    /// <summary>异常</summary>
    public Exception Exception { get; set; }

    /// <summary>原始消息字节。消息长度非法、无法定位边界时，为从失败位置开始的剩余数据</summary>
    public Byte[] Data { get; set; }

    /// <summary>消息在拉取数据包中的起始偏移</summary>
    public Int32 Offset { get; set; }

    /// <summary>消息长度。取自消息头 StoreSize，长度非法时为该非法值</summary>
    public Int32 StoreSize { get; set; }

    /// <summary>友好字符串</summary>
    /// <returns></returns>
    public override String ToString() => $"[{Offset}+{StoreSize}]{Exception?.Message}";
}
