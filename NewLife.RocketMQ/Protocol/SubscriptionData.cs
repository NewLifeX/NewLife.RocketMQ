namespace NewLife.RocketMQ.Protocol
{
    /// <summary>订阅者数据</summary>
    public class SubscriptionData
    {
        #region 属性
        /// <summary>主题</summary>
        public String Topic { get; set; }

        /// <summary>表达式类型</summary>
        public String ExpressionType { get; set; } = "TAG";

        /// <summary>子字符串</summary>
        public String SubString { get; set; } = "*";

        /// <summary>标签集合</summary>
        public String[] TagsSet { get; set; }

        /// <summary>代码集合</summary>
        public String[] CodeSet { get; set; }

        /// <summary>过滤模式</summary>
        public Boolean ClassFilterMode { get; set; }

        /// <summary>过滤源</summary>
        public String FilterClassSource { get; set; }

        /// <summary>子版本</summary>
        public Int64 SubVersion { get; set; } = DateTime.Now.Ticks;
        #endregion
    }
}