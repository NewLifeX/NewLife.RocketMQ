using System;
using System.Collections.Generic;

namespace NewLife.RocketMQ.Protocol
{
    public class QueryResult
    {
        public Int32 IndexLastUpdateTimestamp { get; set; }

        public List<MessageExt> MessageList { get; set; }
    }
}