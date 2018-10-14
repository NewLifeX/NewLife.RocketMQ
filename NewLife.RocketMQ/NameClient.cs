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
        public String Id { get; }

        public IDictionary<String, String> Brokers { get; } = new Dictionary<String, String>();
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
            var rs = Send(RequestCode.GET_ROUTEINTO_BY_TOPIC, new { topic });

            // 解析broker集群地址
            if (rs.Data["brokerDatas"] is IList<Object> bs)
            {
                foreach (IDictionary<String, Object> item in bs)
                {
                    var name = item["brokerName"] + "";
                    if (item["brokerAddrs"] is IDictionary<String, Object> addrs)
                        Brokers[name] = addrs.Join(";", e => e.Value);
                }
            }

            return Brokers;
        }
        #endregion
    }
}