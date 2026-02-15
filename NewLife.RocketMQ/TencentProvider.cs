namespace NewLife.RocketMQ;

/// <summary>腾讯云 TDMQ RocketMQ 适配器</summary>
/// <remarks>
/// 腾讯云 TDMQ 提供 RocketMQ 4.x 兼容实例，支持 Remoting 协议。
/// 签名方式与 Apache ACL 类似，使用 HMAC-SHA1。
/// VPC 内网访问直接配置 NameServer 地址即可。
/// </remarks>
public class TencentProvider : ICloudProvider
{
    /// <summary>提供者名称</summary>
    public String Name => "Tencent";

    /// <summary>访问令牌。腾讯云 SecretId</summary>
    public String AccessKey { get; set; }

    /// <summary>访问密钥。腾讯云 SecretKey</summary>
    public String SecretKey { get; set; }

    /// <summary>通道标识。默认TENCENT</summary>
    public String OnsChannel { get; set; } = "TENCENT";

    /// <summary>命名空间。腾讯云 TDMQ 中的命名空间，用于资源隔离</summary>
    public String Namespace { get; set; }

    /// <summary>转换主题名。腾讯云有命名空间时拼接前缀</summary>
    /// <param name="topic">原始主题名</param>
    /// <returns></returns>
    public String TransformTopic(String topic)
    {
        var ns = Namespace;
        if (!String.IsNullOrEmpty(ns) && !topic.StartsWith(ns))
            return $"{ns}%{topic}";

        return topic;
    }

    /// <summary>转换消费组名。腾讯云有命名空间时拼接前缀</summary>
    /// <param name="group">原始消费组名</param>
    /// <returns></returns>
    public String TransformGroup(String group)
    {
        var ns = Namespace;
        if (!String.IsNullOrEmpty(ns) && !group.StartsWith(ns))
            return $"{ns}%{group}";

        return group;
    }

    /// <summary>获取 NameServer 地址。腾讯云不从HTTP获取，使用 VPC 内网直连</summary>
    public String GetNameServerAddress() => null;
}
