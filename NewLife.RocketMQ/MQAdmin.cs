using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using NewLife.Log;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ.Client
{
    public abstract class MQAdmin : DisposeBase
    {
        #region 属性
        /// <summary>名称服务器地址</summary>
        public String NameServerAddress { get; set; }

        /// <summary>消费组</summary>
        public String Group { get; set; } = "DEFAULT_PRODUCER";

        /// <summary>主题</summary>
        public String Topic { get; set; } = "TBW102";

        /// <summary>本地IP地址</summary>
        public String ClientIP { get; set; } = NetHelper.MyIP() + "";

        /// <summary>本地端口</summary>
        public Int32 ClientPort { get; set; }

        /// <summary>实例名</summary>
        public String InstanceName { get; set; } = "DEFAULT";

        /// <summary>客户端回调执行线程数。默认CPU数</summary>
        public Int32 ClientCallbackExecutorThreads { get; set; } = Environment.ProcessorCount;

        /// <summary>拉取名称服务器间隔。默认30_000ms</summary>
        public Int32 PollNameServerInterval { get; set; } = 30_000;

        /// <summary>Broker心跳间隔。默认30_000ms</summary>
        public Int32 HeartbeatBrokerInterval { get; set; } = 30_000;

        /// <summary>持久化消费偏移间隔。默认5_000ms</summary>
        public Int32 PersistConsumerOffsetInterval { get; set; } = 5_000;

        public String UnitName { get; set; }

        public Boolean UnitMode { get; set; }

        public Boolean VipChannelEnabled { get; set; } = true;

        private ServiceState State { get; set; } = ServiceState.CreateJust;

        /// <summary>队列集合</summary>
        public IList<MessageQueue> Queues { get; private set; }

        private NameClient _Client;
        #endregion

        #region 阿里云属性
        /// <summary>获取名称服务器地址的http地址</summary>
        public String Server { get; set; }

        /// <summary>访问令牌</summary>
        public String AccessKey { get; set; }

        /// <summary>访问密钥</summary>
        public String SecretKey { get; set; }

        /// <summary>阿里云MQ通道</summary>
        public String OnsChannel { get; set; } = "ALIYUN";
        #endregion

        #region 扩展属性
        public String ClientId
        {
            get
            {
                var str = $"{ClientIP}@{InstanceName}";
                if (!UnitName.IsNullOrEmpty()) str += "@" + UnitName;
                return str;
            }
        }
        #endregion

        #region 扩展
        public MQAdmin()
        {
            InstanceName = Process.GetCurrentProcess().Id + "";
        }
        #endregion

        #region 基础方法
        public virtual void Start()
        {
            // 获取阿里云ONS的名称服务器地址
            var addr = Server;
            if (!addr.IsNullOrEmpty() && addr.StartsWithIgnoreCase("http"))
            {
                var http = new HttpClient();
                var rs = http.GetStringAsync(addr).Result;
                if (!rs.IsNullOrWhiteSpace()) NameServerAddress = rs.Trim();
            }

            switch (State)
            {
                case ServiceState.CreateJust:
                    State = ServiceState.CreateJust;

                    var client = new NameClient(ClientId, this);
                    client.Start();

                    var rs = client.GetRouteInfo(Topic);
                    foreach (var item in rs)
                    {
                        XTrace.WriteLine("发现Broker[{0}]: {1}", item.Key, item.Value);
                    }

                    _Client = client;
                    Queues = client.Queues;

                    State = ServiceState.Running;
                    break;
                case ServiceState.Running:
                case ServiceState.ShutdownAlready:
                case ServiceState.StartFailed:
                    throw new Exception("已启动！");
            }
        }
        #endregion

        #region 收发信息
        private BrokerClient _Broker;
        protected BrokerClient GetBroker(String name = null)
        {
            if (_Broker != null) return _Broker;

            var bk = _Client.Brokers?.FirstOrDefault(e => name == null || e.Key == name);
            if (bk == null) return null;

            var addr = bk.Value.Value?.Split(";").FirstOrDefault();
            if (addr.IsNullOrEmpty()) return null;

            var client = new BrokerClient(addr)
            {
                Config = this,
                Id = ClientId,
            };
            client.Start();

            return _Broker = client;
        }
        #endregion
    }
}