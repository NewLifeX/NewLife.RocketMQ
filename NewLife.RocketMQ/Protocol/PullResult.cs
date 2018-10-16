using System;
using System.Collections.Generic;

namespace NewLife.RocketMQ.Protocol
{
    /// <summary>拉取状态</summary>
    public enum PullStatus
    {
        /// <summary>已发现</summary>
        Found = 0,

        /// <summary>没有新的消息</summary>
        NoNewMessage = 1,

        /// <summary>没有批评消息</summary>
        NoMatchedMessage = 2,

        /// <summary>偏移量非法</summary>
        OffsetIllegal = 3
    }

    /// <summary>拉取结果</summary>
    public class PullResult
    {
        #region 属性
        /// <summary>状态</summary>
        public PullStatus Status { get; set; }

        /// <summary>最小偏移</summary>
        public Int64 MinOffset { get; set; }

        /// <summary>最大偏移</summary>
        public Int64 MaxOffset { get; set; }

        /// <summary>下一轮拉取偏移</summary>
        public Int64 NextBeginOffset { get; set; }

        /// <summary>消息</summary>
        public MessageExt[] Messages { get; set; }
        #endregion

        #region 方法
        /// <summary>读取数据</summary>
        /// <param name="dic"></param>
        public void Read(IDictionary<String, String> dic)
        {
            if (dic.TryGetValue(nameof(MinOffset), out var str)) MinOffset = str.ToLong();
            if (dic.TryGetValue(nameof(MaxOffset), out str)) MaxOffset = str.ToLong();
            if (dic.TryGetValue(nameof(NextBeginOffset), out str)) NextBeginOffset = str.ToLong();
        }
        #endregion
    }
}