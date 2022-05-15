namespace NewLife.RocketMQ.Protocol
{
    /// <summary>生产者数据</summary>
    public class ProducerData
    {
        #region 属性
        /// <summary>组名</summary>
        public String GroupName { get; set; } = "CLIENT_INNER_PRODUCER";
        #endregion
    }
}