using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ;

/// <summary>云厂商适配器接口。统一各云厂商的签名认证和实例路由逻辑</summary>
public interface ICloudProvider
{
    /// <summary>提供者名称</summary>
    String Name { get; }

    /// <summary>访问令牌</summary>
    String AccessKey { get; }

    /// <summary>访问密钥</summary>
    String SecretKey { get; }

    /// <summary>通道标识</summary>
    String OnsChannel { get; }

    /// <summary>转换主题名。用于阿里云等需要加实例ID前缀的场景</summary>
    /// <param name="topic">原始主题名</param>
    /// <returns>转换后的主题名</returns>
    String TransformTopic(String topic);

    /// <summary>转换消费组名</summary>
    /// <param name="group">原始消费组名</param>
    /// <returns>转换后的消费组名</returns>
    String TransformGroup(String group);

    /// <summary>获取 NameServer 地址。用于从 HTTP 接口获取地址的场景</summary>
    /// <returns>NameServer 地址，null 表示无需特殊处理</returns>
    String GetNameServerAddress();
}
