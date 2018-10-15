using System;
using System.Collections.Generic;
using System.Linq;
using NewLife.Log;
using NewLife.Net;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ
{
    public class NameClient : MQClient
    {
        #region 属性
        /// <summary>Broker集合</summary>
        public IDictionary<String, String> Brokers { get; } = new Dictionary<String, String>();

        /// <summary>队列集合</summary>
        public IList<MessageQueue> Queues { get; } = new List<MessageQueue>();
        #endregion

        #region 构造
        public NameClient(String id, MQAdmin config)
        {
            Id = id;
            Config = config;
        }
        #endregion

        #region 方法
        protected override NetUri GetServer()
        {
            var cfg = Config;
            var ss = cfg.NameServerAddress.Split(";");

            XTrace.WriteLine("连接NameServer[{0}]", ss[0]);

            return new NetUri(ss[0]);
        }
        #endregion

        #region 命令
        /// <summary>获取主题的路由信息，含登录验证</summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        public IDictionary<String, String> GetRouteInfo(String topic)
        {
            // 发送命令
            var rs = Send(RequestCode.GET_ROUTEINTO_BY_TOPIC, null, new { topic });
            var js = rs.ReadBodyAsJson();

            // 解析broker集群地址
            if (js["brokerDatas"] is IList<Object> bs)
            {
                Brokers.Clear();

                foreach (IDictionary<String, Object> item in bs)
                {
                    var name = item["brokerName"] + "";
                    if (item["brokerAddrs"] is IDictionary<String, Object> addrs)
                        Brokers[name] = addrs.Join(";", e => e.Value);
                }
            }
            // 解析队列集合
            if (js["queueDatas"] is IList<Object> bs2)
            {
                Queues.Clear();

                foreach (IDictionary<String, Object> item in bs2)
                {
                    var name = item["brokerName"] + "";
                    var perm = item["perm"].ToInt();
                    var readQueueNums = item["readQueueNums"].ToInt();
                    var writeQueueNums = item["writeQueueNums"].ToInt();
                    var topicSynFlag = item["topicSynFlag"].ToInt();

                    if ((perm & 4) == 4)
                    {
                        for (var i = 0; i < readQueueNums; i++)
                        {
                            var mq = new MessageQueue
                            {
                                Topic = topic,
                                BrokerName = name,
                                QueueId = i,
                            };

                            Queues.Add(mq);
                        }
                    }
                }
            }

            return Brokers;
        }
        #endregion
    }
}