namespace NewLife.RocketMQ.Protocol
{
    /// <summary>心跳数据</summary>
    public class HeartbeatData
    {
        #region 属性
        /// <summary>客户端编号</summary>
        public String ClientID { get; set; }

        /// <summary>消费数据集</summary>
        public ConsumerData[] ConsumerDataSet { get; set; }

        /// <summary>生产者数据集</summary>
        public ProducerData[] ProducerDataSet { get; set; }
        #endregion
    }
}