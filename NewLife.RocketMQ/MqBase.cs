using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using NewLife.Log;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ.Client;

/// <summary>业务基类</summary>
public abstract class MqBase : DisposeBase
{
    #region 属性
    /// <summary>名称服务器地址</summary>
    public String NameServerAddress { get; set; }

    private String _group = "DEFAULT_PRODUCER";
    /// <summary>消费组</summary>
    /// <remarks>阿里云目前需要在Group前面带上实例ID并用【%】连接,组成路由Group[用来路由到实例Group]</remarks>
    public String Group
    {
        get
        {
            // 阿里云目前需要在Group前面带上实例ID并用【%】连接,组成路由Group[用来路由到实例Group]
            var ins = Aliyun?.InstanceId;
            return ins.IsNullOrEmpty() ? _group : $"{ins}%{_group}";
        }
        set
        {
            _group = value;
        }
    }

    private String _topic = "TBW102";
    /// <summary>主题</summary>
    /// <remarks>阿里云目前需要在Topic前面带上实例ID并用【%】连接,组成路由Topic[用来路由到实例Topic]</remarks>
    public String Topic
    {
        get
        {
            // 阿里云目前需要在Topic前面带上实例ID并用【%】连接,组成路由Topic[用来路由到实例Topic]
            var ins = Aliyun?.InstanceId;
            return ins.IsNullOrEmpty() ? _topic : $"{ins}%{_topic}";
        }
        set
        {
            _topic = value;
        }
    }

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

    /// <summary>序列化类型。默认Json，支持RocketMQ二进制</summary>
    public SerializeType SerializeType { get; set; } = SerializeType.JSON;

    //public Boolean VipChannelEnabled { get; set; } = true;

    /// <summary>是否可用</summary>
    public Boolean Active { get; private set; }

    /// <summary>代理集合</summary>
    public IList<BrokerInfo> Brokers => _NameServer?.Brokers.OrderBy(t => t.Name).ToList();

    /// <summary>阿里云选项。使用阿里云RocketMQ的参数有些不一样</summary>
    public AliyunOptions Aliyun { get; set; }

    /// <summary> Apache RocketMQ ACL 客户端配置。在Borker服务器配置设置为AclEnable = true 时配置生效。</summary>
    public AclOptions AclOptions { get; set; }

    /// <summary>性能跟踪</summary>
    public ITracer Tracer { get; set; } = DefaultTracer.Instance;

    /// <summary>名称服务器</summary>
    protected NameClient _NameServer;
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
    static MqBase()
    {
        // 输出当前版本
        Assembly.GetExecutingAssembly().WriteVersion();
    }

    /// <summary>实例化</summary>
    public MqBase()
    {
        InstanceName = Process.GetCurrentProcess().Id + "";

        // 设置UnitName，避免一个进程多实例时冲突
        //UnitName = Rand.Next() + "";
    }

    /// <summary>销毁</summary>
    /// <param name="disposing"></param>
    protected override void Dispose(Boolean disposing)
    {
        base.Dispose(disposing);

        _NameServer.TryDispose();

        //foreach (var item in _Brokers)
        //{
        //    item.Value.TryDispose();
        //}
        Stop();
    }

    /// <summary>友好字符串</summary>
    /// <returns></returns>
    public override String ToString() => Group;
    #endregion

    #region 基础方法
    /// <summary>应用配置</summary>
    /// <param name="setting"></param>
    public virtual void Configure(MqSetting setting)
    {
        NameServerAddress = setting.NameServer;
        Topic = setting.Topic;
        Group = setting.Group;

        if (!setting.Server.IsNullOrEmpty() &&
            !setting.AccessKey.IsNullOrEmpty())
        {
            Aliyun = new AliyunOptions
            {
                Server = setting.Server,
                AccessKey = setting.AccessKey,
                SecretKey = setting.SecretKey,
            };
        }
    }

    /// <summary>开始</summary>
    /// <returns></returns>
    public virtual Boolean Start()
    {
        if (Active) return true;

        if (NameServerAddress.IsNullOrEmpty())
        {
            // 获取阿里云ONS的名称服务器地址
            var addr = Aliyun?.Server;
            if (!addr.IsNullOrEmpty() && addr.StartsWithIgnoreCase("http"))
            {
                var http = new System.Net.Http.HttpClient();
                var html = http.GetStringAsync(addr).Result;

                if (!html.IsNullOrWhiteSpace()) NameServerAddress = html.Trim();
            }
        }

        WriteLog("正在从名称服务器[{0}]查找该Topic所在Broker服务器地址列表", NameServerAddress);

        var client = new NameClient(ClientId, this)
        {
            Name = Topic,
            Tracer = Tracer,
            Log = Log
        };
        client.Start();

        // 阻塞获取Broker地址，确保首次使用之前已经获取到Broker地址
        var rs = client.GetRouteInfo(Topic);
        foreach (var item in rs)
        {
            XTrace.WriteLine("发现Broker[{0}]: {1}, reads={2}, writes={3}", item.Name, item.Addresses.Join(), item.ReadQueueNums, item.WriteQueueNums);
        }

        _NameServer = client;

        return Active = true;
    }

    /// <summary>停止</summary>
    /// <returns></returns>
    public virtual void Stop()
    {
        if (!Active) return;

        foreach (var item in _Brokers)
        {
            try
            {
                item.Value.UnRegisterClient(Group);
                item.Value.TryDispose();
            }
            catch (Exception ex)
            {
                XTrace.WriteException(ex);
            }
        }
        _Brokers.Clear();

        Active = false;
    }
    #endregion

    #region 收发信息
    private readonly ConcurrentDictionary<String, BrokerClient> _Brokers = new();
    /// <summary>获取代理客户端</summary>
    /// <param name="name"></param>
    /// <returns></returns>
    protected BrokerClient GetBroker(String name)
    {
        if (String.IsNullOrEmpty(name)) throw new ArgumentException($"“{nameof(name)}”不能为 null 或空。", nameof(name));

        if (_Brokers.TryGetValue(name, out var client)) return client;

        var bk = Brokers?.FirstOrDefault(e => name == null || e.Name == name);
        if (bk == null) return null;

        lock (_Brokers)
        {
            if (_Brokers.TryGetValue(name, out client)) return client;

            // 实例化客户端
            client = CreateBroker(bk.Name, bk.Addresses);

            client.Start();

            // 尝试添加
            _Brokers.TryAdd(name, client);

            return client;
        }
    }

    /// <summary>创建Broker客户端通信</summary>
    /// <param name="name"></param>
    /// <param name="addrs"></param>
    /// <returns></returns>
    protected virtual BrokerClient CreateBroker(String name, String[] addrs)
    {
        var client = new BrokerClient(addrs)
        {
            Id = ClientId,
            Name = name,
            Config = this,

            Tracer = Tracer,
            Log = ClientLog,
        };

        client.Received += (s, e) =>
        {
            e.Arg = OnReceive(e.Arg);
        };

        return client;
    }

    /// <summary>Broker客户端集合</summary>
    public ICollection<BrokerClient> Clients => _Brokers.Values;

    /// <summary>收到命令</summary>
    /// <param name="cmd"></param>
    protected virtual Command OnReceive(Command cmd) => null;
    #endregion

    #region 业务方法
    /// <summary>更新或创建主题。重复执行时为更新</summary>
    /// <param name="topic">主题</param>
    /// <param name="queueNum">队列数</param>
    /// <param name="topicSysFlag"></param>
    public virtual void CreateTopic(String topic, Int32 queueNum, Int32 topicSysFlag = 0)
    {
        var header = new
        {
            topic,
            defaultTopic = Topic,
            readQueueNums = queueNum,
            writeQueueNums = queueNum,
            perm = 7,
            topicFilterType = "SINGLE_TAG",
            topicSysFlag,
            order = false,
        };

        using var span = Tracer?.NewSpan($"mq:{Topic}:CreateTopic", header);
        try
        {
            // 在所有Broker上创建Topic
            foreach (var item in Brokers)
            {
                WriteLog("在Broker[{0}]上创建主题：{1}", item.Name, topic);
                try
                {
                    var bk = GetBroker(item.Name);
                    var rs = bk.Invoke(RequestCode.UPDATE_AND_CREATE_TOPIC, null, header);
                }
                catch (Exception ex)
                {
                    XTrace.WriteException(ex);
                }
            }
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);

            throw;
        }
    }
    #endregion

    #region 日志
    /// <summary>日志</summary>
    public ILog Log { get; set; } = Logger.Null;

    /// <summary>客户端日志。详细的指令收发日志，仅用于调试</summary>
    public ILog ClientLog { get; set; }

    /// <summary>写日志</summary>
    /// <param name="format"></param>
    /// <param name="args"></param>
    public void WriteLog(String format, params Object[] args) => Log?.Info($"[{this}]" + format, args);
    #endregion
}