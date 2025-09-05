using NewLife.Data;
using NewLife.Log;
using NewLife.Net;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;
using NewLife.Threading;

namespace NewLife.RocketMQ;

/// <summary>连接名称服务器的客户端</summary>
public class NameClient : ClusterClient
{
    #region 属性

    /// <summary>Broker集合</summary>
    public IList<BrokerInfo> Brokers { get; private set; } = [];

    /// <summary>代理改变时触发</summary>
    public event EventHandler OnBrokerChange;

    #endregion

    #region 构造

    /// <summary>实例化</summary>
    /// <param name="id"></param>
    /// <param name="config"></param>
    public NameClient(String id, MqBase config)
    {
        Id = id;
        Config = config;
    }
    #endregion

    #region 方法

    /// <inheritdoc/>
    protected override void Dispose(Boolean disposing)
    {
        if (disposing)
            _timer?.Dispose();

        base.Dispose(disposing);
    }

    /// <summary>启动</summary>
    protected override void OnStart()
    {
        var cfg = Config;
        if (cfg.NameServerAddress.IsNullOrEmpty())
            throw new ArgumentNullException(nameof(cfg.NameServerAddress), "未指定NameServer地址");

        var ss = cfg.NameServerAddress.Split(";");

        var list = new List<NetUri>();
        foreach (var item in ss)
        {
            var uri = new NetUri(item);
            if (uri.Type == NetType.Unknown) uri.Type = NetType.Tcp;
            list.Add(uri);
        }

        Servers = list.ToArray();

        base.OnStart();

        _timer ??= new TimerX(DoWork, null, cfg.PollNameServerInterval, cfg.PollNameServerInterval) { Async = true };
    }

    #endregion

    #region 命令

    private TimerX _timer;
    private String _lastBrokers;
    private void DoWork(Object state)
    {
        var rs = GetRouteInfo(Config.Topic);
        var str = rs?.Join(",", e => $"{e.Name}={e.Addresses.Join()}");
        if (str != _lastBrokers)
        {
            _lastBrokers = str;
            foreach (var item in rs)
            {
                WriteLog("发现Broker[{0}]: {1}, reads={2}, writes={3}", item.Name, item.Addresses.Join(), item.ReadQueueNums, item.WriteQueueNums);
            }
        }
    }

    /// <summary>获取主题的路由信息，含登录验证</summary>
    /// <param name="topic"></param>
    /// <returns></returns>
    public IList<BrokerInfo> GetRouteInfo(String topic)
    {
        using var span = Tracer?.NewSpan($"mq:{topic}:GetRouteInfo", topic);
        try
        {
            // 发送命令
            var rs = Invoke(RequestCode.GET_ROUTEINTO_BY_TOPIC, null, new { topic });
            span?.AppendTag(rs.Payload?.ToStr());
            var js = rs.ReadBodyAsJson();

            var list = new List<BrokerInfo>();
            // 解析broker集群地址
            if (js["brokerDatas"] is IList<Object> bs)
            {
                foreach (IDictionary<String, Object> item in bs)
                {
                    var name = item["brokerName"] + "";
                    var cluster = item["cluster"] + "";
                    if (item["brokerAddrs"] is IDictionary<String, Object> addrs)
                    {
                        // key==0为Master
                        var addresses = addrs.Select(e => e.Value + "").ToArray();
                        var isMaster = addrs.ContainsKey("0");
                        list.Add(new BrokerInfo
                        {
                            Name = name,
                            Cluster = cluster,
                            Addresses = addresses,
                            IsMaster = isMaster
                        });
                    }
                }
            }

            // 解析队列集合
            if (js["queueDatas"] is IList<Object> bs2)
            {
                foreach (IDictionary<String, Object> item in bs2)
                {
                    var name = item["brokerName"] + "";

                    var bk = list.FirstOrDefault(e => e.Name == name);
                    if (bk == null) list.Add(bk = new BrokerInfo { Name = name });

                    bk.Permission = (Permissions)item["perm"].ToInt();
                    bk.ReadQueueNums = item["readQueueNums"].ToInt();
                    bk.WriteQueueNums = item["writeQueueNums"].ToInt();
                    bk.TopicSynFlag = item["topicSynFlag"].ToInt();
                }
            }

            // 如果完全相等，则直接返回。否则重新平衡队列
            if (Brokers.SequenceEqual(list)) return list.OrderBy(t => t.Name).ToList();

            Brokers = list;

            // 结果检查
            if (list.Count == 0)
            {
                WriteLog("未能找到主题[{0}]的任何Broker信息，可能是Topic或NameServer错误，也可能是不支持的服务端版本。服务端返回如下：", topic);
                WriteLog(rs.Payload.ToStr());
            }

            // 有改变，重新平衡队列
            OnBrokerChange?.Invoke(this, EventArgs.Empty);

            return list.OrderBy(t => t.Name).ToList();
        }
        catch (ResponseException ex)
        {
            if (!topic.Equals(MqBase.DefaultTopic) && ResponseCode.TOPIC_NOT_EXIST.Equals(ex.Code))
            {
                WriteLog("未能找到主题[{0}]，将读取默认主题[TBW102]的替代。", topic);
                var rs = GetRouteInfo(MqBase.DefaultTopic);
                if (rs != null && rs.Count() > 0)
                {
                    if (rs[0].ReadQueueNums > Config.DefaultTopicQueueNums)
                    {
                        foreach (var item in rs)
                        {
                            item.WriteQueueNums = Config.DefaultTopicQueueNums;
                            item.ReadQueueNums = Config.DefaultTopicQueueNums;
                        }
                    }
                }
                return rs;
            }
            else
            {
                span?.SetError(ex, null);
                throw;
            }
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }
    }

    #endregion
}