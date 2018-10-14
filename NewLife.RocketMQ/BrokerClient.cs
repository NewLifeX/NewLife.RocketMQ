using System;
using NewLife.Net;

namespace NewLife.RocketMQ
{
    public class BrokerClient : MQClient
    {
        #region 属性
        /// <summary>服务器地址</summary>
        public String Server { get; }
        #endregion

        #region 构造
        public BrokerClient(String server) => Server = server;
        #endregion

        #region 方法
        protected override NetUri GetServer() => new NetUri(Server);
        #endregion
    }
}