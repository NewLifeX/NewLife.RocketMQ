using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NewLife.RocketMQ.Client
{
    public class QueryResult
    {
        public Int32 IndexLastUpdateTimestamp { get; set; }

        public List<MessageExt> MessageList { get; set; }
    }
}
