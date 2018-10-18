using System;

namespace NewLife.RocketMQ.Protocol
{
    /// <summary>心跳数据</summary>
    public class HeartbeatData
    {
        #region 属性
        /// <summary>客户端编号</summary>
        public String ClientID { get; set; }

        /// <summary>消费数据集</summary>
        public ConsumerData[] ConsumerDataSet { get; set; }

        /// <summary>生产者数据集</summary>
        public ProducerData[] ProducerDataSet { get; set; }
        #endregion
    }

    /// <summary>生产者数据</summary>
    public class ProducerData
    {
        #region 属性
        /// <summary>组名</summary>
        public String GroupName { get; set; } = "CLIENT_INNER_PRODUCER";
        #endregion
    }

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