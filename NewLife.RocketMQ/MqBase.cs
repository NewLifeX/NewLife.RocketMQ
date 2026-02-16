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
    public MQVersion Version { get; set; } = MQVersion.V4_9_7;

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

    /// <summary>是否启用VIP通道。启用后使用Broker端口-2作为VIP通道连接，获得更高优先级，默认false</summary>
    /// <remarks>
    /// RocketMQ的VIP通道使用Broker监听端口减2的端口，例如Broker端口为10911时VIP端口为10909。
    /// VIP通道在高负载场景下可获得更高的处理优先级。
    /// </remarks>
    public Boolean VipChannelEnabled { get; set; }

    /// <summary>是否可用</summary>
    public Boolean Active { get; private set; }

    /// <summary>代理集合</summary>
    public IList<BrokerInfo> Brokers => _NameServer?.Brokers.OrderBy(t => t.Name).ToList();

    /// <summary>云厂商适配器。用于统一签名认证和实例路由逻辑</summary>
    /// <remarks>
    /// 支持阿里云（AliyunProvider）、腾讯云（TencentProvider）、Apache ACL（AclProvider）等。
    /// 设置后自动处理签名、实例ID路由等厂商特有逻辑。
    /// </remarks>
    public ICloudProvider CloudProvider { get; set; }

    /// <summary>阿里云选项。使用阿里云RocketMQ的参数有些不一样</summary>
    [Obsolete("请使用 CloudProvider = new AliyunProvider { ... } 替代")]
    public AliyunOptions Aliyun
    {
        get => _aliyunOptions;
        set
        {
            _aliyunOptions = value;
            // 自动同步到 CloudProvider
            if (value != null && !value.AccessKey.IsNullOrEmpty())
                CloudProvider ??= AliyunProvider.FromOptions(value);
        }
    }
    private AliyunOptions _aliyunOptions;

    /// <summary>Apache RocketMQ ACL 客户端配置。在Borker服务器配置设置为AclEnable = true 时配置生效。</summary>
    [Obsolete("请使用 CloudProvider = new AclProvider { ... } 替代")]
    public AclOptions AclOptions
    {
        get => _aclOptions;
        set
        {
            _aclOptions = value;
            if (value != null && !value.AccessKey.IsNullOrEmpty())
                CloudProvider ??= AclProvider.FromOptions(value);
        }
    }
    private AclOptions _aclOptions;

    /// <summary>Json序列化主机</summary>
    public IJsonHost JsonHost { get; set; } = JsonHelper.Default;

    /// <summary>性能追踪器</summary>
    public ITracer Tracer { get; set; } = DefaultTracer.Instance;

    /// <summary>是否启用消息轨迹</summary>
    public Boolean EnableMessageTrace { get; set; }

#if NETSTANDARD2_1_OR_GREATER
    /// <summary>gRPC Proxy地址。设置后使用gRPC协议连接RocketMQ 5.x</summary>
    /// <remarks>
    /// 格式如 http://host:8081 或 https://host:8081。
    /// 设置此属性后，将使用gRPC协议替代Remoting协议，支持RocketMQ 5.x新特性。
    /// </remarks>
    public String GrpcProxyAddress { get; set; }

    /// <summary>gRPC消息服务客户端</summary>
    protected Grpc.GrpcMessagingService _GrpcService;
#endif

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

#if NETSTANDARD2_1_OR_GREATER
        _GrpcService.TryDispose();
#endif
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

        // 兼容旧版配置方式
#pragma warning disable CS0618
        Aliyun ??= new AliyunOptions();
        if (!setting.Server.IsNullOrEmpty()) Aliyun.Server = setting.Server;
        if (!setting.AccessKey.IsNullOrEmpty()) Aliyun.AccessKey = setting.AccessKey;
        if (!setting.SecretKey.IsNullOrEmpty()) Aliyun.SecretKey = setting.SecretKey;
#pragma warning restore CS0618
    }

    /// <summary>开始</summary>
    /// <returns></returns>
    public Boolean Start()
    {
        if (Active) return true;

        _group = Group;
        _topic = Topic;
        if (Name.IsNullOrEmpty()) Name = Topic;

        // 解析阿里云实例ID（兼容旧版 AliyunOptions）
        if (CloudProvider is AliyunProvider ap)
        {
            var ns = NameServerAddress;
            if (ap.InstanceId.IsNullOrEmpty() && !ns.IsNullOrEmpty() && ns.Contains("MQ_INST_"))
            {
                ap.InstanceId = ns.Substring("://", ".");
            }
        }
#pragma warning disable CS0618
        else if (_aliyunOptions != null && !_aliyunOptions.AccessKey.IsNullOrEmpty())
        {
            var ns = NameServerAddress;
            if (_aliyunOptions.InstanceId.IsNullOrEmpty() && !ns.IsNullOrEmpty() && ns.Contains("MQ_INST_"))
            {
                _aliyunOptions.InstanceId = ns.Substring("://", ".");
            }
        }
#pragma warning restore CS0618

        using var span = Tracer?.NewSpan($"mq:{Name}:Start");
        try
        {
            // 通过 CloudProvider 转换 Topic/Group
            var provider = CloudProvider;
            if (provider != null)
            {
                Topic = provider.TransformTopic(Topic);
                Group = provider.TransformGroup(Group);
            }
#pragma warning disable CS0618
            else
            {
                // 兼容旧版：阿里云实例ID前缀
                var ins = _aliyunOptions?.InstanceId;
                if (!ins.IsNullOrEmpty())
                {
                    if (!Topic.StartsWith(ins)) Topic = $"{ins}%{Topic}";
                    if (!Group.StartsWith(ins)) Group = $"{ins}%{Group}";
                }
            }
#pragma warning restore CS0618

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
#if NETSTANDARD2_1_OR_GREATER
        // 使用gRPC协议时，初始化gRPC客户端
        if (!GrpcProxyAddress.IsNullOrEmpty())
        {
            WriteLog("使用gRPC协议连接Proxy[{0}]", GrpcProxyAddress);

            var svc = new Grpc.GrpcMessagingService(GrpcProxyAddress)
            {
                Namespace = CloudProvider is AliyunProvider ap ? ap.InstanceId : null,
                Log = Log,
                Tracer = Tracer,
            };

            // 设置认证信息
            if (CloudProvider != null && !CloudProvider.AccessKey.IsNullOrEmpty())
            {
                svc.Client.AccessKey = CloudProvider.AccessKey;
                svc.Client.SecretKey = CloudProvider.SecretKey;
            }
            svc.Client.ClientId = ClientId;

            // 查询路由验证连通性
            var route = svc.QueryRouteAsync(Topic).ConfigureAwait(false).GetAwaiter().GetResult();
            if (route.Status?.Code != Grpc.GrpcCode.OK)
                throw new InvalidOperationException($"gRPC QueryRoute failed: {route.Status}");

            WriteLog("gRPC路由查询成功，发现[{0}]个队列", route.MessageQueues.Count);
            _GrpcService = svc;
            return;
        }
#endif

        if (NameServerAddress.IsNullOrEmpty())
        {
            // 通过 CloudProvider 获取 NameServer 地址
            var addr = CloudProvider?.GetNameServerAddress();
            if (!addr.IsNullOrEmpty())
            {
                NameServerAddress = addr;
            }
#pragma warning disable CS0618
            else
            {
                // 兼容旧版：从阿里云 HTTP 接口获取
                var server = _aliyunOptions?.Server;
                if (!server.IsNullOrEmpty() && server.StartsWithIgnoreCase("http"))
                {
                    var http = new System.Net.Http.HttpClient();
                    var html = http.GetStringAsync(server).ConfigureAwait(false).GetAwaiter().GetResult();

                    if (!html.IsNullOrWhiteSpace()) NameServerAddress = html.Trim();
                }
            }
#pragma warning restore CS0618
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
            WriteLog("发现Broker[{0}]: {1}, reads={2}, writes={3}", item.Name, item.Addresses.Join(), item.ReadQueueNums, item.WriteQueueNums);
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

    /// <summary>删除主题</summary>
    /// <param name="topic">主题</param>
    public virtual Int32 DeleteTopic(String topic)
    {
        var count = 0;
        using var span = Tracer?.NewSpan($"mq:{Name}:DeleteTopic", topic);
        try
        {
            // 从所有Broker上删除
            foreach (var item in Brokers)
            {
                WriteLog("在Broker[{0}]上删除主题：{1}", item.Name, topic);
                try
                {
                    var bk = GetBroker(item.Name);
                    var rs = bk.Invoke(RequestCode.DELETE_TOPIC_IN_BROKER, null, new { topic });
                    if (rs != null && rs.Header.Code == (Int32)ResponseCode.SUCCESS) count++;
                }
                catch (Exception ex)
                {
                    XTrace.WriteException(ex);
                }
            }

            // 从NameServer上删除
            try
            {
                _NameServer?.Invoke(RequestCode.DELETE_TOPIC_IN_NAMESRV, null, new { topic });
            }
            catch (Exception ex)
            {
                XTrace.WriteException(ex);
            }
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }

        return count;
    }

    /// <summary>创建或更新消费组</summary>
    /// <param name="groupName">消费组名</param>
    /// <param name="consumeBroadcastEnable">是否允许广播消费</param>
    /// <param name="retryMaxTimes">最大重试次数</param>
    /// <param name="retryQueueNums">重试队列数</param>
    public virtual Int32 CreateSubscriptionGroup(String groupName, Boolean consumeBroadcastEnable = true, Int32 retryMaxTimes = 16, Int32 retryQueueNums = 1)
    {
        var count = 0;
        using var span = Tracer?.NewSpan($"mq:{Name}:CreateSubscriptionGroup", groupName);
        try
        {
            var header = new
            {
                groupName,
                consumeBroadcastEnable,
                consumeEnable = true,
                retryMaxTimes,
                retryQueueNums,
            };

            foreach (var item in Brokers)
            {
                WriteLog("在Broker[{0}]上创建消费组：{1}", item.Name, groupName);
                try
                {
                    var bk = GetBroker(item.Name);
                    var rs = bk.Invoke(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null, header);
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

    /// <summary>删除消费组</summary>
    /// <param name="groupName">消费组名</param>
    public virtual Int32 DeleteSubscriptionGroup(String groupName)
    {
        var count = 0;
        using var span = Tracer?.NewSpan($"mq:{Name}:DeleteSubscriptionGroup", groupName);
        try
        {
            foreach (var item in Brokers)
            {
                WriteLog("在Broker[{0}]上删除消费组：{1}", item.Name, groupName);
                try
                {
                    var bk = GetBroker(item.Name);
                    var rs = bk.Invoke(RequestCode.DELETE_SUBSCRIPTIONGROUP, null, new { groupName });
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

    /// <summary>按消息ID查看消息</summary>
    /// <param name="msgId">消息编号</param>
    /// <returns></returns>
    public virtual MessageExt ViewMessage(String msgId)
    {
        using var span = Tracer?.NewSpan($"mq:{Name}:ViewMessage", msgId);
        try
        {
            foreach (var item in Brokers)
            {
                try
                {
                    var bk = GetBroker(item.Name);
                    var rs = bk.Invoke(RequestCode.VIEW_MESSAGE_BY_ID, null, new { offset = msgId }, true);
                    if (rs?.Payload != null)
                    {
                        var msgs = MessageExt.ReadAll(rs.Payload);
                        if (msgs?.Count > 0) return msgs[0];
                    }
                }
                catch { }
            }
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }

        return null;
    }

    /// <summary>获取集群信息</summary>
    /// <returns></returns>
    public virtual IDictionary<String, Object> GetClusterInfo()
    {
        using var span = Tracer?.NewSpan($"mq:{Name}:GetClusterInfo");
        try
        {
            var rs = _NameServer?.Invoke(RequestCode.GET_BROKER_CLUSTER_INFO, null);
            if (rs?.Payload != null) return rs.ReadBodyAsJson();
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }

        return null;
    }

    /// <summary>获取消费统计信息</summary>
    /// <param name="group">消费组名</param>
    /// <param name="topic">主题。默认使用当前Topic</param>
    /// <returns>消费统计数据的JSON字典</returns>
    public virtual IDictionary<String, Object> GetConsumeStats(String group, String topic = null)
    {
        if (String.IsNullOrEmpty(group)) group = Group;
        if (String.IsNullOrEmpty(topic)) topic = Topic;

        using var span = Tracer?.NewSpan($"mq:{Name}:GetConsumeStats", group);
        try
        {
            foreach (var item in Brokers)
            {
                try
                {
                    var bk = GetBroker(item.Name);
                    var rs = bk.Invoke(RequestCode.GET_CONSUME_STATS, null, new
                    {
                        consumerGroup = group,
                        topic,
                    }, true);
                    if (rs?.Payload != null) return rs.ReadBodyAsJson();
                }
                catch { }
            }
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }

        return null;
    }

    /// <summary>获取Topic统计信息</summary>
    /// <param name="topic">主题。默认使用当前Topic</param>
    /// <returns>主题统计数据的JSON字典</returns>
    public virtual IDictionary<String, Object> GetTopicStatsInfo(String topic = null)
    {
        if (String.IsNullOrEmpty(topic)) topic = Topic;

        using var span = Tracer?.NewSpan($"mq:{Name}:GetTopicStatsInfo", topic);
        try
        {
            foreach (var item in Brokers)
            {
                try
                {
                    var bk = GetBroker(item.Name);
                    var rs = bk.Invoke(RequestCode.GET_TOPIC_STATS_INFO, null, new { topic }, true);
                    if (rs?.Payload != null) return rs.ReadBodyAsJson();
                }
                catch { }
            }
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }

        return null;
    }

    /// <summary>按Key查询消息</summary>
    /// <param name="topic">主题</param>
    /// <param name="key">消息Key</param>
    /// <param name="maxNum">最大返回数量</param>
    /// <param name="beginTimestamp">起始时间戳（毫秒）</param>
    /// <param name="endTimestamp">结束时间戳（毫秒）</param>
    /// <returns>匹配的消息列表</returns>
    public virtual IList<MessageExt> QueryMessageByKey(String topic, String key, Int32 maxNum = 32, Int64 beginTimestamp = 0, Int64 endTimestamp = 0)
    {
        if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        if (String.IsNullOrEmpty(topic)) topic = Topic;

        using var span = Tracer?.NewSpan($"mq:{Name}:QueryMessageByKey", key);
        try
        {
            foreach (var item in Brokers)
            {
                try
                {
                    var bk = GetBroker(item.Name);
                    var rs = bk.Invoke(RequestCode.QUERY_MESSAGE, null, new
                    {
                        topic,
                        key,
                        maxNum,
                        beginTimestamp,
                        endTimestamp,
                    }, true);
                    if (rs?.Payload != null)
                    {
                        var msgs = MessageExt.ReadAll(rs.Payload);
                        if (msgs?.Count > 0) return msgs;
                    }
                }
                catch { }
            }
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }

        return [];
    }

    /// <summary>注册消息过滤服务器。将一个外部过滤服务器注册到Broker上，用于服务端消息过滤</summary>
    /// <param name="filterServerAddr">过滤服务器地址，格式如 ip:port</param>
    /// <returns>注册成功的Broker数量</returns>
    public virtual Int32 RegisterFilterServer(String filterServerAddr)
    {
        if (String.IsNullOrEmpty(filterServerAddr)) throw new ArgumentNullException(nameof(filterServerAddr));

        var count = 0;
        using var span = Tracer?.NewSpan($"mq:{Name}:RegisterFilterServer", filterServerAddr);
        try
        {
            foreach (var item in Brokers)
            {
                WriteLog("在Broker[{0}]上注册过滤服务器：{1}", item.Name, filterServerAddr);
                try
                {
                    var bk = GetBroker(item.Name);
                    var rs = bk.Invoke(RequestCode.REGISTER_FILTER_SERVER, null, new { filterServerAddr }, true);
                    if (rs != null) count++;
                }
                catch (Exception ex)
                {
                    WriteLog("注册过滤服务器失败[{0}]：{1}", item.Name, ex.Message);
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

#if NETSTANDARD2_1_OR_GREATER
    #region gRPC公共方法
    /// <summary>通过gRPC协议查询主题路由</summary>
    /// <param name="topic">主题。默认使用当前Topic</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>路由查询结果</returns>
    public async Task<Grpc.QueryRouteResponse> QueryRouteViaGrpcAsync(String topic = null, CancellationToken cancellationToken = default)
    {
        if (_GrpcService == null) throw new InvalidOperationException("gRPC service not initialized. Set GrpcProxyAddress first.");

        return await _GrpcService.QueryRouteAsync(topic ?? Topic, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>通过gRPC协议上报客户端资源信息（Telemetry）</summary>
    /// <param name="settings">客户端设置</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>服务端返回的Telemetry命令</returns>
    public async Task<Grpc.TelemetryCommand> TelemetryViaGrpcAsync(Grpc.GrpcSettings settings, CancellationToken cancellationToken = default)
    {
        if (_GrpcService == null) throw new InvalidOperationException("gRPC service not initialized. Set GrpcProxyAddress first.");

        using var span = Tracer?.NewSpan($"mq:{Name}:Telemetry:grpc");
        try
        {
            return await _GrpcService.TelemetryAsync(settings, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }
    }

    /// <summary>通过gRPC协议通知客户端终止</summary>
    /// <param name="group">消费组</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<Grpc.NotifyClientTerminationResponse> NotifyClientTerminationViaGrpcAsync(String group = null, CancellationToken cancellationToken = default)
    {
        if (_GrpcService == null) throw new InvalidOperationException("gRPC service not initialized. Set GrpcProxyAddress first.");

        using var span = Tracer?.NewSpan($"mq:{Name}:NotifyTermination:grpc");
        try
        {
            return await _GrpcService.NotifyClientTerminationAsync(group ?? Group, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }
    }
    #endregion
#endif

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