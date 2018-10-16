using System;

namespace NewLife.RocketMQ.Protocol
{
    /// <summary>消息扩展</summary>
    public class MessageExt : Message
    {
        #region 属性
        /// <summary>队列编号</summary>
        public Int32 QueueId { get; set; }

        /// <summary>生产时间</summary>
        public Int32 BornTimestamp { get; set; }

        /// <summary>生产主机</summary>
        public String BornHost { get; set; }

        /// <summary>存储时间</summary>
        public Int64 StoreTimestamp { get; set; }

        /// <summary>存储主机</summary>
        public String StoreHost { get; set; }

        /// <summary>消息编号</summary>
        public String MsgId { get; set; }
        #endregion
    }
}