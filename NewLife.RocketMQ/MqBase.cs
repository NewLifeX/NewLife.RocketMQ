using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using NewLife.Http;
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

        ///// <summary>本地端口</summary>
        //public Int32 ClientPort { get; set; }

        /// <summary>实例名</summary>
        public String InstanceName { get; set; } = "DEFAULT";

        ///// <summary>客户端回调执行线程数。默认CPU数</summary>
        //public Int32 ClientCallbackExecutorThreads { get; set; } = Environment.ProcessorCount;

        /// <summary>拉取名称服务器间隔。默认30_000ms</summary>
        public Int32 PollNameServerInterval { get; set; } = 30_000;

        /// <summary>Broker心跳间隔。默认30_000ms</summary>
        public Int32 HeartbeatBrokerInterval { get; set; } = 30_000;

        /// <summary>单元名称</summary>
        public String UnitName { get; set; }

        /// <summary>单元模式</summary>
        public Boolean UnitMode { get; set; }

        //public Boolean VipChannelEnabled { get; set; } = true;

        /// <summary>是否可用</summary>
        public Boolean Active { get; private set; }

        /// <summary>代理集合</summary>
        public IList<BrokerInfo> Brokers => _NameServer?.Brokers;

        /// <summary>名称服务器</summary>
        protected NameClient _NameServer;
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

        #region 构造
        /// <summary>实例化</summary>
        public MqBase()
        {
            InstanceName = Process.GetCurrentProcess().Id + "";

            // 设置UnitName，避免一个进程多实例时冲突
            //UnitName = Rand.Next() + "";
        }

        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            _NameServer.TryDispose();

            foreach (var item in _Brokers)
            {
                item.Value.TryDispose();
            }
        }

        /// <summary>友好字符串</summary>
        /// <returns></returns>
        public override String ToString() => Group;
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
                var http = new TinyHttpClient();
                var html = http.GetStringAsync(addr).Result;

                if (!html.IsNullOrWhiteSpace()) NameServerAddress = html.Trim();
            }

            var client = new NameClient(ClientId, this);
            client.Start();

            var rs = client.GetRouteInfo(Topic);
            foreach (var item in rs)
            {
                XTrace.WriteLine("发现Broker[{0}]: {1}", item.Name, item.Addresses.Join());
            }

            _NameServer = client;

            return Active = true;
        }
        #endregion

        #region 收发信息
        private readonly ConcurrentDictionary<String, BrokerClient> _Brokers = new ConcurrentDictionary<String, BrokerClient>();
        /// <summary>获取代理客户端</summary>
        /// <param name="name"></param>
        /// <returns></returns>
        protected BrokerClient GetBroker(String name)
        {
            if (_Brokers.TryGetValue(name, out var client)) return client;

            var bk = Brokers?.FirstOrDefault(e => name == null || e.Name == name);
            if (bk == null) return null;

            lock (_Brokers)
            {
                if (_Brokers.TryGetValue(name, out client)) return client;

                // 实例化客户端
                client = new BrokerClient(bk.Addresses)
                {
                    Id = ClientId,
                    Name = bk.Name,
                    Config = this,
                    Log = Log,
                };

                client.Received += (s, e) =>
                {
                    e.Arg = OnReceive(e.Arg);
                };

                client.Start();

                // 尝试添加
                _Brokers.TryAdd(name, client);

                return client;
            }
        }

        /// <summary>Broker客户端集合</summary>
        public ICollection<BrokerClient> Clients => _Brokers.Values;

        /// <summary>收到命令</summary>
        /// <param name="cmd"></param>
        protected virtual Command OnReceive(Command cmd) => null;
        #endregion

        #region 日志
        /// <summary>日志</summary>
        public ILog Log { get; set; } = Logger.Null;

        /// <summary>写日志</summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        public void WriteLog(String format, params Object[] args) => Log?.Info($"[{this}]" + format, args);
        #endregion
    }
}