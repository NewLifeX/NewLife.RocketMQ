using System;
using System.Collections.Generic;

namespace NewLife.RocketMQ.Protocol
{
    /// <summary>查询结果</summary>
    public class QueryResult
    {
        /// <summary>最后更新时间</summary>
        public Int32 IndexLastUpdateTimestamp { get; set; }

        /// <summary>消息列表</summary>
        public List<MessageExt> MessageList { get; set; }
    }
}