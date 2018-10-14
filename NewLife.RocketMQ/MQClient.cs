using System;
using System.Net.Sockets;
using NewLife.Net;
using NewLife.RocketMQ.Client;

namespace NewLife.RocketMQ
{
    class MQClient : DisposeBase
    {
        #region 属性
        public String Id { get; }

        public MQAdmin Config { get; }

        private TcpClient _Client;
        #endregion

        #region 构造
        public MQClient(String id, MQAdmin config)
        {
            Id = id;
            Config = config;
        }
        #endregion

        #region 方法
        public void Start()
        {
            var cfg = Config;
            var ss = cfg.NameServerAddress.Split(";");
            var uri = new NetUri(ss[0]);

            var client = new TcpClient();
            //client.Connect(uri.EndPoint);

            var timeout = 3_000;
            // 采用异步来解决连接超时设置问题
            var ar = client.BeginConnect(uri.Address, uri.Port, null, null);
            if (!ar.AsyncWaitHandle.WaitOne(timeout, false))
            {
                client.Close();
                throw new TimeoutException($"连接[{uri}][{timeout}ms]超时！");
            }

            _Client = client;
        }
        #endregion
    }
}