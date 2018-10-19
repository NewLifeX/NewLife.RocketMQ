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
        /// <summary>相等比较</summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override Boolean Equals(Object obj)
        {
            var x = this;
            if (!(obj is MessageQueue y)) return false;

            return x.Topic == y.Topic && x.BrokerName == y.BrokerName && x.QueueId == y.QueueId;
        }

        /// <summary>计算哈希</summary>
        /// <returns></returns>
        public override Int32 GetHashCode()
        {
            var obj = this;
            return obj.Topic.GetHashCode() ^ obj.BrokerName.GetHashCode() ^ obj.BrokerName.GetHashCode();
        }
        #endregion

        #region 辅助
        /// <summary>友好字符串</summary>
        /// <returns></returns>
        public override String ToString() => $"MessageQueue [Topic={Topic}, BrokerName={BrokerName}, QueueId={QueueId}]";
        #endregion
    }
}