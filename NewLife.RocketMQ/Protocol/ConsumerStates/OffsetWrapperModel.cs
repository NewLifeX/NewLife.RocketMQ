using System;
using System.Collections.Generic;
using System.Text;

namespace NewLife.RocketMQ.Protocol.ConsumerStates
{
    /// <summary>
    /// 消费点位信息模型
    /// </summary>
    public class OffsetWrapperModel
    {
        /// <summary>
        /// 代理者位点
        /// </summary>
        public Int64 BrokerOffset { get; set; }

        /// <summary>
        /// 消费者点位
        /// </summary>
        public Int64 ConsumerOffset { get; set; }

        /// <summary>
        /// 上次时间
        /// </summary>
        public Int64 LastTimestamp { get; set; }

        /// <summary>
        /// 阿里版拉取点位
        /// </summary>
        public Int64 PullOffset { get; set; }
    }
}
