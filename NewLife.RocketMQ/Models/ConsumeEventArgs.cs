using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ.Models
{
    /// <summary>消费事件参数</summary>
    public class ConsumeEventArgs : EventArgs
    {
        /// <summary>队列</summary>
        public MessageQueue Queue { get; set; }

        /// <summary>消息集合</summary>
        public MessageExt[] Messages { get; set; }

        /// <summary>结果</summary>
        public PullResult Result { get; set; }
    }
}