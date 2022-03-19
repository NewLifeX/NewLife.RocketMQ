namespace NewLife.RocketMQ
{
    /// <summary>
    /// 支持 Apache RocketMQ ACL机制
    /// </summary>
    public class AclOptions
    {
        /// <summary>Acl访问令牌</summary>
        public String AccessKey { get; set; }

        /// <summary>Acl访问密钥</summary>
        public String SecretKey { get; set; }

        /// <summary>通道</summary>
        public String OnsChannel { get; set; } = "LOCAL";
    }
}
