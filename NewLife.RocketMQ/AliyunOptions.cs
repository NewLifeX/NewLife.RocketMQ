using System;

namespace NewLife.RocketMQ
{
    /// <summary>
    /// 阿里云选项
    /// </summary>
    public class AliyunOptions
    {
        #region 阿里云属性
        /// <summary>获取名称服务器地址的http地址。阿里云专用</summary>
        public String Server { get; set; } = "http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet";

        /// <summary>访问令牌。阿里云专用</summary>
        public String AccessKey { get; set; }

        /// <summary>访问密钥。阿里云专用</summary>
        public String SecretKey { get; set; }

        /// <summary>实例ID。阿里云专用</summary>
        public String InstanceId { get; set; }

        /// <summary>阿里云MQ通道。阿里云专用</summary>
        public String OnsChannel { get; set; } = "ALIYUN";
        #endregion

    }
}