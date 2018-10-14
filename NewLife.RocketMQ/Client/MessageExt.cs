using System;

namespace NewLife.RocketMQ.Client
{
    public class MessageExt
    {
        #region 属性
        public Int32 QueueId { get; set; }

        public Int32 BornTimestamp { get; set; }

        public String BornHost { get; set; }

        public Int64 StoreTimestamp { get; set; }

        public String StoreHost { get; set; }

        public String MsgId { get; set; }
        #endregion
    }
}