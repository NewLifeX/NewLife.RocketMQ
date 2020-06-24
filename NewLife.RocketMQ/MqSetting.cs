using NewLife.Configuration;
using System;
using System.ComponentModel;

namespace NewLife.RocketMQ
{
    /// <summary>RocketMQ配置</summary>
    [Config("RocketMQ")]
    public class MqSetting : Config<MqSetting>
    {
        /// <summary>获取名称服务器地址的http地址</summary>
        [Description("获取名称服务器地址的http地址")]
        public String Server { get; set; } = "http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet";

        /// <summary>访问令牌</summary>
        [Description("访问令牌")]
        public String AccessKey { get; set; }

        /// <summary>访问密钥</summary>
        [Description("访问密钥")]
        public String SecretKey { get; set; }
    }
}