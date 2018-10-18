using System;
using System.Linq;
using NewLife.Net;
using NewLife.RocketMQ.Protocol;
using NewLife.Threading;

namespace NewLife.RocketMQ
{
    /// <summary>代理客户端</summary>
    public class BrokerClient : ClusterClient
    {
        #region 属性
        /// <summary>服务器地址</summary>
        private readonly String[] _Servers;
        #endregion

        #region 构造
        /// <summary>实例化代理客户端</summary>
        /// <param name="servers"></param>
        public BrokerClient(String[] servers) => _Servers = servers;
        #endregion

        #region 方法
        /// <summary>启动</summary>
        public override void Start()
        {
            Servers = _Servers.Select(e => new NetUri(e)).ToArray();

            base.Start();

            // 心跳
            StartPing();
        }
        #endregion

        #region 注销
        /// <summary>注销客户端</summary>
        /// <param name="id"></param>
        /// <param name="group"></param>
        public virtual void UnRegisterClient(String id, String group)
        {
            if (group.IsNullOrEmpty()) group = "CLIENT_INNER_PRODUCER";

            var rs = Invoke(RequestCode.UNREGISTER_CLIENT, new
            {
                ClientId = id,
                ProducerGroup = group,
                ConsumerGroup = group,
            });
        }
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
            Ping();
        }

        /// <summary>心跳</summary>
        public void Ping()
        {
            var cfg = Config;

            Invoke(RequestCode.HEART_BEAT, new
            {
                ClientID = Id,
                ProducerDataSet = new[] {
                   new{ GroupName="DEFAULT_PRODUCER" },
                   new{ GroupName=cfg.Group },
                },
            }, null);
        }
        #endregion
    }
}