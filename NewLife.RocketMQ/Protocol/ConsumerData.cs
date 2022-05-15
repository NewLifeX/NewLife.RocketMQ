namespace NewLife.RocketMQ.Protocol
{
    /// <summary>消费者数据</summary>
    public class ConsumerData
    {
        #region 属性
        /// <summary>从哪里开始消费</summary>
        public String ConsumeFromWhere { get; set; } = "CONSUME_FROM_LAST_OFFSET";

        /// <summary>消费类型</summary>
        public String ConsumeType { get; set; } = "CONSUME_ACTIVELY";

        /// <summary>组名</summary>
        public String GroupName { get; set; }

        /// <summary>消息模型。广播/集群</summary>
        public String MessageModel { get; set; } = "CLUSTERING";

        /// <summary>订阅数据集</summary>
        public SubscriptionData[] SubscriptionDataSet { get; set; }

        /// <summary>单元模式</summary>
        public Boolean UnitMode { get; set; }
        #endregion
    }
}