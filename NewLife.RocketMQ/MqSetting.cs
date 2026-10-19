using System.ComponentModel;
using NewLife.Configuration;

namespace NewLife.RocketMQ;

/// <summary>RocketMQ配置</summary>
[Config("RocketMQ")]
public class MqSetting : Config<MqSetting>
{
    /// <summary>名称服务器。将从该地址获取Broker</summary>
    [Description("NameServer address. Brokers are discovered from it.")]
    public String NameServer { get; set; }

    /// <summary>主题</summary>
    [Description("Topic")]
    public String Topic { get; set; }

    /// <summary>消费组</summary>
    [Description("Consumer group")]
    public String Group { get; set; }

    /// <summary>获取名称服务器地址的http地址，阿里云专用 http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet</summary>
    [Description("HTTP address that returns the NameServer address; Aliyun only. http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet")]
    public String Server { get; set; }

    /// <summary>访问令牌，阿里云专用</summary>
    [Description("Access token; Aliyun only.")]
    public String AccessKey { get; set; }

    /// <summary>访问密钥，阿里云专用</summary>
    [Description("Secret key; Aliyun only.")]
    public String SecretKey { get; set; }
}