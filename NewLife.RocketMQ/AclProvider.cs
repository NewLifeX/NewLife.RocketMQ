namespace NewLife.RocketMQ;

/// <summary>Apache RocketMQ ACL 适配器</summary>
public class AclProvider : ICloudProvider
{
    /// <summary>提供者名称</summary>
    public String Name => "ACL";

    /// <summary>访问令牌</summary>
    public String AccessKey { get; set; }

    /// <summary>访问密钥</summary>
    public String SecretKey { get; set; }

    /// <summary>通道标识。默认空</summary>
    public String OnsChannel { get; set; } = "";

    #region ACL 2.0 权限字段（RocketMQ 5.3+，可选）
    /// <summary>是否启用 ACL 2.0 资源级权限。默认false（使用ACL 1.x HMAC签名即可）。
    /// 启用后会在请求头中追加 aclEnabled/resourceType/resourceName 字段，需 RocketMQ 5.3+ Broker 支持。
    /// 与 ACL 1.x HMAC-SHA1 签名向后兼容，两套机制可同时生效。</summary>
    public Boolean AclEnabled { get; set; }

    /// <summary>ACL 2.0 资源类型。1=Topic，2=Group。仅当 AclEnabled=true 时生效</summary>
    public Int32 ResourceType { get; set; }

    /// <summary>ACL 2.0 资源名称。对应 Topic 名或 ConsumerGroup 名。仅当 AclEnabled=true 时生效</summary>
    public String ResourceName { get; set; }
    #endregion

    /// <summary>转换主题名。ACL模式不转换</summary>
    public String TransformTopic(String topic) => topic;

    /// <summary>转换消费组名。ACL模式不转换</summary>
    public String TransformGroup(String group) => group;

    /// <summary>获取 NameServer 地址。ACL模式不从HTTP获取</summary>
    public String GetNameServerAddress() => null;

    /// <summary>从旧版 AclOptions 创建</summary>
    /// <param name="options">旧版ACL选项</param>
    /// <returns></returns>
    public static AclProvider FromOptions(AclOptions options)
    {
        if (options == null) return null;

        return new AclProvider
        {
            AccessKey = options.AccessKey,
            SecretKey = options.SecretKey,
            OnsChannel = options.OnsChannel ?? "",
        };
    }
}
