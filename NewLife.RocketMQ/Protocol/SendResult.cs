using System;
using System.Collections.Generic;

namespace NewLife.RocketMQ.Protocol
{
    /// <summary>发送状态</summary>
    public enum SendStatus
    {
        SendOK = 0,
        FlushDiskTimeout = 1,
        FlushSlaveTimeout = 2,
        SlaveNotAvailable = 3,
    }

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
        public void Read(IDictionary<String, String> dic)
        {
            if (dic.TryGetValue(nameof(MsgId), out var str)) MsgId = str;
            if (dic.TryGetValue(nameof(QueueOffset), out str)) QueueOffset = str.ToLong();
            if (dic.TryGetValue(nameof(RegionId), out str)) RegionId = str;
            if (dic.TryGetValue("MSG_REGION", out str)) RegionId = str;
        }
        #endregion
    }
}