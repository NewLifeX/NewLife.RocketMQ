using System;

namespace NewLife.RocketMQ.Protocol
{
    /// <summary>消息队列</summary>
    public class MessageQueue
    {
        #region 属性
        /// <summary>主题</summary>
        public String Topic { get; set; }

        /// <summary>代理名称</summary>
        public String BrokerName { get; set; }

        /// <summary>队列编号</summary>
        public Int32 QueueId { get; set; }
        #endregion

        #region 相等
        #endregion

        #region 辅助
        /// <summary>友好字符串</summary>
        /// <returns></returns>
        public override String ToString() => $"MessageQueue [Topic={Topic}, BrokerName={BrokerName}, QueueId={QueueId}]";
        #endregion
    }
}