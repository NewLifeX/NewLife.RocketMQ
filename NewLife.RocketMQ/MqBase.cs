using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Xml.Serialization;
using NewLife.Log;
using NewLife.Net;
using NewLife.RocketMQ.Protocol;
using NewLife.Serialization;

namespace NewLife.RocketMQ.Client;

/// <summary>业务基类</summary>
public abstract class MqBase : DisposeBase
{
    #region 属性
    /// <summary>名称</summary>
    public String Name { get; set; }

    /// <summary>名称服务器地址</summary>
    public String NameServerAddress { get; set; }

    /// <summary>消费组</summary>
    /// <remarks>阿里云目前需要在Group前面带上实例ID并用【%】连接,组成路由Group[用来路由到实例Group]</remarks>
    public String Group { get; set; } = "DEFAULT_PRODUCER";

    /// <summary>rocketmq 默认主题</summary>
    public static String DefaultTopic { get; } = "TBW102";

    /// <summary>主题</summary>
    /// <remarks>阿里云目前需要在Topic前面带上实例ID并用【%】连接,组成路由Topic[用来路由到实例Topic]</remarks>
    public String Topic { get; set; } = DefaultTopic;

    /// <summary>默认的主题队列数量</summary>
    public Int32 DefaultTopicQueueNums { get; set; } = 4;

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

    /// <summary>让通信层知道对方的版本号，响应方可以以此做兼容老版本等的特殊操作</summary>
    public MQVersion Version { get; set; } = MQVersion.V4_8_0;

    /// <summary>SSL协议。默认None</summary>
    public SslProtocols SslProtocol { get; set; } = SslProtocols.None;

    /// <summary>X509证书。用于SSL连接时验证证书指纹，可以直接加载pem证书文件，未指定时不验证证书</summary>
    /// <remarks>
    /// 可以使用pfx证书文件，也可以使用pem证书文件。
    /// 服务端必须指定证书。
    /// </remarks>
    /// <example>
    /// var cert = new X509Certificate2("file", "pass");
    /// </example>
    public X509Certificate? Certificate { get; set; }

    /// <summary>是否使用外部代理。有些RocketMQ的Broker部署在网关外部，需要使用映射地址，默认false</summary>
    public Boolean ExternalBroker { get; set; }

    //public Boolean VipChannelEnabled { get; set; } = true;

    /// <summary>是否可用</summary>
    public Boolean Active { get; private set; }

    /// <summary>代理集合</summary>
    public IList<BrokerInfo> Brokers => _NameServer?.Brokers.OrderBy(t => t.Name).ToList();

    /// <summary>阿里云选项。使用阿里云RocketMQ的参数有些不一样</summary>
    public AliyunOptions Aliyun { get; set; }

    /// <summary> Apache RocketMQ ACL 客户端配置。在Borker服务器配置设置为AclEnable = true 时配置生效。</summary>
    public AclOptions AclOptions { get; set; }

    /// <summary>Json序列化主机</summary>
    public IJsonHost JsonHost { get; set; } = JsonHelper.Default;

    /// <summary>性能跟踪</summary>
    public ITracer Tracer { get; set; } = DefaultTracer.Instance;

    private String _group;
    private String _topic;

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

        XTrace.WriteLine("RocketMQ文档：https://newlifex.com/core/rocketmq");
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
    public override String ToString() => _group;
    #endregion

    #region 基础方法
    /// <summary>应用配置</summary>
    /// <param name="setting"></param>
    public virtual void Configure(MqSetting setting)
    {
        if (!setting.NameServer.IsNullOrEmpty()) NameServerAddress = setting.NameServer;
        if (!setting.Topic.IsNullOrEmpty()) Topic = setting.Topic;
        if (!setting.Group.IsNullOrEmpty()) Group = setting.Group;

        Aliyun ??= new AliyunOptions();
        if (!setting.Server.IsNullOrEmpty()) Aliyun.Server = setting.Server;
        if (!setting.AccessKey.IsNullOrEmpty()) Aliyun.AccessKey = setting.AccessKey;
        if (!setting.SecretKey.IsNullOrEmpty()) Aliyun.SecretKey = setting.SecretKey;
    }

    /// <summary>开始</summary>
    /// <returns></returns>
    public Boolean Start()
    {
        if (Active) return true;

        _group = Group;
        _topic = Topic;
        if (Name.IsNullOrEmpty()) Name = Topic;

        // 解析阿里云实例
        var aliyun = Aliyun;
        if (aliyun != null && !aliyun.AccessKey.IsNullOrEmpty())
        {
            var ns = NameServerAddress;
            if (aliyun.InstanceId.IsNullOrEmpty() && !ns.IsNullOrEmpty() && ns.Contains("MQ_INST_"))
            {
                aliyun.InstanceId = ns.Substring("://", ".");
            }
        }

        using var span = Tracer?.NewSpan($"mq:{Name}:Start");
        try
        {
            // 阿里云目前需要在Topic前面带上实例ID并用【%】连接,组成路由Topic[用来路由到实例Topic]
            var ins = Aliyun?.InstanceId;
            if (!ins.IsNullOrEmpty())
            {
                if (!Topic.StartsWith(ins)) Topic = $"{ins}%{Topic}";
                if (!Group.StartsWith(ins)) Group = $"{ins}%{Group}";
            }

            OnStart();
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);

            throw;
        }

        return Active = true;
    }

    /// <summary>开始</summary>
    protected virtual void OnStart()
    {
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
            Name = Name,
            Tracer = Tracer,
            Log = Log
        };
        client.Start();

        // 阻塞获取Broker地址，确保首次使用之前已经获取到Broker地址
        var rs = client.GetRouteInfo(Topic);
        DefaultTopicQueueNums = Math.Min(DefaultTopicQueueNums, rs.Where(e => e.Permission.HasFlag(Permissions.Write) && e.WriteQueueNums > 0).Select(e => e.WriteQueueNums).First());

        foreach (var item in rs)
        {
            XTrace.WriteLine("发现Broker[{0}]: {1}, reads={2}, writes={3}", item.Name, item.Addresses.Join(), item.ReadQueueNums, item.WriteQueueNums);
        }

        _NameServer = client;
    }

    /// <summary>停止</summary>
    /// <returns></returns>
    public void Stop()
    {
        if (!Active) return;

        using var span = Tracer?.NewSpan($"mq:{Name}:Stop");
        try
        {
            OnStop();
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);

            throw;
        }

        Active = false;
    }

    /// <summary>停止</summary>
    protected virtual void OnStop()
    {
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

            var addrs = bk.Addresses.ToArray();
            if (ExternalBroker)
            {
                // broker可能在内网，转为公网地址
                var uri = new NetUri(NameServerAddress.Split(";").FirstOrDefault());
                var ext = uri.Host;
                if (ext.IsNullOrEmpty()) ext = uri.Address.ToString();

                for (var i = 0; i < addrs.Length; i++)
                {
                    var addr = addrs[i];
                    if (addr.StartsWithIgnoreCase("127.", "10.", "192.", "172.") && !ext.IsNullOrEmpty())
                    {
                        var p = addr.IndexOf(':');
                        addrs[i] = p > 0 ? ext + addr[p..] : ext;
                    }
                }
            }

            // 实例化客户端
            client = CreateBroker(bk.Name, addrs);

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
    public virtual Int32 CreateTopic(String topic, Int32 queueNum, Int32 topicSysFlag = 0)
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

        var count = 0;
        using var span = Tracer?.NewSpan($"mq:{Name}:CreateTopic", header);
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
                    if (rs != null && rs.Header.Code == (Int32)ResponseCode.SUCCESS) count++;
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

        return count;
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