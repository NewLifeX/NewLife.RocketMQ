using System;
using System.Collections.Generic;

namespace NewLife.RocketMQ.Protocol
{
    /// <summary>发送结果</summary>
    public class SendResult
    {
        #region 属性
        public SendStatus Status { get; set; }

        public String MsgId { get; set; }

        public MessageQueue Queue { get; set; }

        public Int64 QueueOffset { get; set; }

        public String TransactionId { get; set; }

        public String OffsetMsgId { get; set; }

        public String RegionId { get; set; }
        #endregion

        #region 方法
        public void Read(IDictionary<String, Object> dic)
        {
            if (dic.TryGetValue(nameof(MsgId), out var obj)) MsgId = obj + "";
            if (dic.TryGetValue(nameof(QueueOffset), out obj)) QueueOffset = obj.ToLong();
            if (dic.TryGetValue(nameof(RegionId), out obj)) RegionId = obj + "";
            if (dic.TryGetValue("MSG_REGION", out obj)) RegionId = obj + "";
        }
        #endregion
    }
}