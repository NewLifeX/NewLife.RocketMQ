using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NewLife.RocketMQ.Client
{
    /// <summary>消息队列</summary>
    public class MessageQueue
    {
        #region 属性
        public String Topic { get; set; }

        public String BrokerName { get; set; }

        public Int32 QueueId { get; set; }
        #endregion

        #region 相等
        #endregion

        #region 辅助
        public override String ToString() => $"MessageQueue [Topic={Topic}, BrokerName={BrokerName}, QueueId={QueueId}]";
        #endregion
    }
}