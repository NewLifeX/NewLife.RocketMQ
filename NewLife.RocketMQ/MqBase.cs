using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using NewLife.Log;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ.Client
{
    /// <summary>业务基类</summary>
    public abstract class MqBase : DisposeBase
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

        /// <summary>单元名称</summary>
        public String UnitName { get; set; }

        /// <summary>单元模式</summary>
        public Boolean UnitMode { get; set; }

        //public Boolean VipChannelEnabled { get; set; } = true;

        /// <summary>是否可用</summary>
        public Boolean Active { get; private set; }

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
        /// <summary>客户端标识</summary>
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
        /// <summary>实例化</summary>
        public MqBase()
        {
            InstanceName = Process.GetCurrentProcess().Id + "";
        }
        #endregion

        #region 基础方法
        /// <summary>开始</summary>
        /// <returns></returns>
        public virtual Boolean Start()
        {
            if (Active) return true;

            // 获取阿里云ONS的名称服务器地址
            var addr = Server;
            if (!addr.IsNullOrEmpty() && addr.StartsWithIgnoreCase("http"))
            {
                var http = new HttpClient();
                var html = http.GetStringAsync(addr).Result;
                if (!html.IsNullOrWhiteSpace()) NameServerAddress = html.Trim();
            }

            var client = new NameClient(ClientId, this);
            client.Start();

            var rs = client.GetRouteInfo(Topic);
            foreach (var item in rs)
            {
                XTrace.WriteLine("发现Broker[{0}]: {1}", item.Key, item.Value);
            }

            _Client = client;
            Queues = client.Queues;

            return Active = true;
        }
        #endregion

        #region 收发信息
        private BrokerClient _Broker;
        /// <summary>获取代理客户端</summary>
        /// <param name="name"></param>
        /// <returns></returns>
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