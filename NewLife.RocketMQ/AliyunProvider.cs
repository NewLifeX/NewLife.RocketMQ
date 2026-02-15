namespace NewLife.RocketMQ;

/// <summary>阿里云 RocketMQ 适配器</summary>
public class AliyunProvider : ICloudProvider
{
    /// <summary>提供者名称</summary>
    public String Name => "Aliyun";

    /// <summary>访问令牌</summary>
    public String AccessKey { get; set; }

    /// <summary>访问密钥</summary>
    public String SecretKey { get; set; }

    /// <summary>通道标识。默认ALIYUN</summary>
    public String OnsChannel { get; set; } = "ALIYUN";

    /// <summary>实例ID。MQ_INST_xxx</summary>
    public String InstanceId { get; set; }

    /// <summary>NameServer HTTP 发现地址</summary>
    public String Server { get; set; }

    /// <summary>转换主题名。阿里云需要加实例ID前缀</summary>
    /// <param name="topic">原始主题名</param>
    /// <returns></returns>
    public String TransformTopic(String topic)
    {
        var ins = InstanceId;
        if (!String.IsNullOrEmpty(ins) && !topic.StartsWith(ins))
            return $"{ins}%{topic}";

        return topic;
    }

    /// <summary>转换消费组名。阿里云需要加实例ID前缀</summary>
    /// <param name="group">原始消费组名</param>
    /// <returns></returns>
    public String TransformGroup(String group)
    {
        var ins = InstanceId;
        if (!String.IsNullOrEmpty(ins) && !group.StartsWith(ins))
            return $"{ins}%{group}";

        return group;
    }

    /// <summary>获取 NameServer 地址。从阿里云 HTTP 接口获取</summary>
    /// <returns></returns>
    public String GetNameServerAddress()
    {
        var addr = Server;
        if (String.IsNullOrEmpty(addr) || !addr.StartsWith("http", StringComparison.OrdinalIgnoreCase))
            return null;

        var http = new System.Net.Http.HttpClient();
        var html = http.GetStringAsync(addr).ConfigureAwait(false).GetAwaiter().GetResult();

        return String.IsNullOrWhiteSpace(html) ? null : html.Trim();
    }

    /// <summary>从旧版 AliyunOptions 创建</summary>
    /// <param name="options">旧版阿里云选项</param>
    /// <returns></returns>
    public static AliyunProvider FromOptions(AliyunOptions options)
    {
        if (options == null) return null;

        return new AliyunProvider
        {
            AccessKey = options.AccessKey,
            SecretKey = options.SecretKey,
            OnsChannel = options.OnsChannel ?? "ALIYUN",
            InstanceId = options.InstanceId,
            Server = options.Server,
        };
    }
}
