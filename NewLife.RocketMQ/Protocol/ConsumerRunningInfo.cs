using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NewLife.RocketMQ.Protocol
{
    class ConsumerRunningInfo
    {
        #region 属性
        public IDictionary<String,String> Properties { get; set; }

        public SubscriptionData[] SubscriptionSet { get; set; }

        public String[] MqTable { get; set; }
        #endregion
    }
}
