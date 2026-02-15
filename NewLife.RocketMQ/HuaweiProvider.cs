namespace NewLife.RocketMQ;

/// <summary>华为云 DMS RocketMQ 适配器</summary>
public class HuaweiProvider : ICloudProvider
{
    /// <summary>提供者名称</summary>
    public String Name => "Huawei";

    /// <summary>访问令牌</summary>
    public String AccessKey { get; set; }

    /// <summary>访问密钥</summary>
    public String SecretKey { get; set; }

    /// <summary>通道标识。默认HUAWEI</summary>
    public String OnsChannel { get; set; } = "HUAWEI";

    /// <summary>实例ID</summary>
    public String InstanceId { get; set; }

    /// <summary>是否启用SSL</summary>
    public Boolean EnableSsl { get; set; }

    /// <summary>转换主题名。华为云不转换</summary>
    /// <param name="topic">原始主题名</param>
    /// <returns></returns>
    public String TransformTopic(String topic) => topic;

    /// <summary>转换消费组名。华为云不转换</summary>
    /// <param name="group">原始消费组名</param>
    /// <returns></returns>
    public String TransformGroup(String group) => group;

    /// <summary>获取 NameServer 地址。华为云不从HTTP获取</summary>
    /// <returns></returns>
    public String GetNameServerAddress() => null;
}
