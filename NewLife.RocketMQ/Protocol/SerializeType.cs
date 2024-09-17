namespace NewLife.RocketMQ.Protocol;

/// <summary>序列化类型</summary>
public enum SerializeType : Byte
{
    /// <summary>Json序列化</summary>
    JSON = 0,

    /// <summary>二进制序列化</summary>
    ROCKETMQ = 1,
}