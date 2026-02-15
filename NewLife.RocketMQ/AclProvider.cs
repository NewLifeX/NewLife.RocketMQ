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
