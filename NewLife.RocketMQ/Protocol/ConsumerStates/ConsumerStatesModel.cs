using System;
using System.Collections.Generic;
using System.Text;

namespace NewLife.RocketMQ.Protocol.ConsumerStates
{
    /// <summary>
    /// 消费者状态信息模型
    /// </summary>
    public class ConsumerStatesModel
    {
        /// <summary>
        /// 消费Tps
        /// </summary>
        public double ConsumeTps { get; set; }

        /// <summary>
        /// 消费Offset信息Table
        /// </summary>
        public Dictionary<MessageQueueModel,OffsetWrapperModel> OffsetTable { get; set; }
    }
}
