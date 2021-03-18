using NewLife.Configuration;
using System;
using System.ComponentModel;

namespace NewLife.RocketMQ
{
    /// <summary>RocketMQ配置</summary>
    [Config("RocketMQ")]
    public class MqSetting : Config<MqSetting>
    {
        /// <summary>名称服务器。将从该地址获取Broker</summary>
        [Description("名称服务器。将从该地址获取Broker")]
        public String NameServer { get; set; }

        /// <summary>主题</summary>
        [Description("主题")]
        public String Topic { get; set; }

        /// <summary>消费组</summary>
        [Description("消费组")]
        public String Group { get; set; }

        /// <summary>获取名称服务器地址的http地址，阿里云专用 http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet</summary>
        [Description("获取名称服务器地址的http地址，阿里云专用 http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet")]
        public String Server { get; set; }

        /// <summary>访问令牌，阿里云专用</summary>
        [Description("访问令牌，阿里云专用")]
        public String AccessKey { get; set; }

        /// <summary>访问密钥，阿里云专用</summary>
        [Description("访问密钥，阿里云专用")]
        public String SecretKey { get; set; }
    }
}