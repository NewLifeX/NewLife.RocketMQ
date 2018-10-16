using System;
using NewLife.Net;
using NewLife.RocketMQ.Protocol;
using NewLife.Threading;

namespace NewLife.RocketMQ
{
    /// <summary>代理客户端</summary>
    public class BrokerClient : MQClient
    {
        #region 属性
        /// <summary>服务器地址</summary>
        public String Server { get; }
        #endregion

        #region 构造
        /// <summary>实例化代理客户端</summary>
        /// <param name="server"></param>
        public BrokerClient(String server) => Server = server;
        #endregion

        #region 方法
        /// <summary>启动</summary>
        public override void Start()
        {
            base.Start();

            // 心跳
            StartPing();
        }

        /// <summary>获取服务器地址</summary>
        /// <returns></returns>
        protected override NetUri GetServer() => new NetUri(Server);
        #endregion

        #region 心跳
        private TimerX _timer;

        private void StartPing()
        {
            if (_timer == null)
            {
                var period = Config.HeartbeatBrokerInterval;

                _timer = new TimerX(OnPing, null, 100, period);
            }
        }

        private void OnPing(Object state)
        {
            var cfg = Config;

            Send(RequestCode.HEART_BEAT, new
            {
                ClientId = Id,
                ProducerDataSet = new[] {
                   new{ GroupName=cfg.Group },
                },
            });
        }
        #endregion
    }
}