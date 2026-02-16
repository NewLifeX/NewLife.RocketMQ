using NewLife.Log;
using NewLife.Net;
using NewLife.RocketMQ.Protocol;
using NewLife.Threading;

namespace NewLife.RocketMQ;

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
    protected override void OnStart()
    {
        //Servers = _Servers.Select(e => new NetUri(e)).ToArray();
        var list = new List<NetUri>();
        foreach (var item in _Servers)
        {
            var uri = new NetUri(item);
            if (uri.Type == NetType.Unknown) uri.Type = NetType.Tcp;

            // VIP通道使用端口-2
            if (Config != null && Config.VipChannelEnabled && uri.Port > 2)
                uri.Port -= 2;

            list.Add(uri);
        }
        Servers = list.ToArray();

        base.OnStart();

        // 心跳
        StartPing();
    }
    #endregion

    #region 注销
    /// <summary>注销客户端</summary>
    /// <param name="group"></param>
    public virtual Command UnRegisterClient(String group)
    {
        if (group.IsNullOrEmpty()) group = "CLIENT_INNER_PRODUCER";

        return Invoke(RequestCode.UNREGISTER_CLIENT, new
        {
            ClientId = Id,
            ProducerGroup = group,
            ConsumerGroup = group,
        });
    }

    /// <inheritdoc/>

    protected override void Dispose(Boolean disposing)
    {
        if (disposing)
            _timer?.Dispose();

        base.Dispose(disposing);
    }
    #endregion

    #region 心跳
    private TimerX _timer;

    private void StartPing()
    {
        if (_timer == null)
        {
            var period = Config.HeartbeatBrokerInterval;

            _timer = new TimerX(OnPing, null, 100, period) { Async = true };
        }
    }

    private void OnPing(Object state)
    {
        DefaultSpan.Current = null;

        Ping();
    }

    /// <summary>心跳</summary>
    public void Ping()
    {
        using var span = Tracer?.NewSpan($"mq:{Name}:Ping");
        try
        {
            var cfg = Config;

            var body = new HeartbeatData { ClientID = Id };

            // 生产者 和 消费者 略有不同
            if (cfg is Producer pd)
            {
                body.ProducerDataSet = [
                new ProducerData { GroupName = pd.Group },
                new ProducerData { GroupName = "CLIENT_INNER_PRODUCER" },
            ];
                body.ConsumerDataSet = [];
            }
            else if (cfg is Consumer cm)
            {
                body.ProducerDataSet = [new ProducerData { GroupName = "CLIENT_INNER_PRODUCER" }];
                body.ConsumerDataSet = cm.Data.ToArray();
            }

            span?.AppendTag(body);

            // 心跳忽略错误。有时候报40错误
            Invoke(RequestCode.HEART_BEAT, body, null, true);
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);

            if (ex.GetTrue() is not TaskCanceledException)
                throw;
        }
    }
    #endregion

    #region 运行信息
    /// <summary>获取运行时信息</summary>
    /// <returns></returns>
    public IDictionary<String, Object> GetRuntimeInfo()
    {
        var rs = Invoke(RequestCode.GET_BROKER_RUNTIME_INFO, null);
        if (rs == null || rs.Payload == null) return null;

        var dic = rs.ReadBodyAsJson();

        return dic?["table"] as IDictionary<String, Object>;
    }
    #endregion
}