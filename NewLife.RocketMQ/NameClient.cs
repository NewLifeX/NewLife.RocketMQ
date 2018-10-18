using System;
using System.Collections.Generic;
using System.Linq;
using NewLife.Net;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;
using NewLife.Threading;

namespace NewLife.RocketMQ
{
    /// <summary>连接名称服务器的客户端</summary>
    public class NameClient : ClusterClient
    {
        #region 属性
        /// <summary>Broker集合</summary>
        public IList<BrokerInfo> Brokers { get; } = new List<BrokerInfo>();

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
        /// <summary>启动</summary>
        public override void Start()
        {
            var cfg = Config;
            var ss = cfg.NameServerAddress.Split(";");

            Servers = ss.Select(e => new NetUri(e)).ToArray();

            base.Start();

            if (_timer == null) _timer = new TimerX(DoWork, null, cfg.PollNameServerInterval, cfg.PollNameServerInterval);
        }
        #endregion

        #region 命令
        private TimerX _timer;
        private void DoWork(Object state) => GetRouteInfo(Config.Topic);

        /// <summary>获取主题的路由信息，含登录验证</summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        public IList<BrokerInfo> GetRouteInfo(String topic)
        {
            // 发送命令
            var rs = Invoke(RequestCode.GET_ROUTEINTO_BY_TOPIC, null, new { topic });
            var js = rs.ReadBodyAsJson();

            var list = new List<BrokerInfo>();
            // 解析broker集群地址
            if (js["brokerDatas"] is IList<Object> bs)
            {
                foreach (IDictionary<String, Object> item in bs)
                {
                    var name = item["brokerName"] + "";
                    if (item["brokerAddrs"] is IDictionary<String, Object> addrs)
                        list.Add(new BrokerInfo { Name = name, Addresses = addrs.Select(e => e.Value + "").ToArray() });
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
            if (Brokers.SequenceEqual(list)) return list;

            Brokers.Clear();
            if (Brokers is List<BrokerInfo> bks) bks.AddRange(list);

            // 有改变，重新平衡队列
            //_brokers = null;
            //_robin = null;
            OnBrokerChange?.Invoke(this, EventArgs.Empty);

            return list;
        }
        #endregion
    }
}