using System;
using System.Collections.Generic;
using System.Linq;
using NewLife.Log;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ
{
    /// <summary>消费者</summary>
    public class Consumer : MqBase
    {
        #region 属性
        /// <summary>数据</summary>
        public IList<ConsumerData> Data { get; set; }

        /// <summary>持久化消费偏移间隔。默认5_000ms</summary>
        public Int32 PersistConsumerOffsetInterval { get; set; } = 5_000;
        #endregion

        #region 方法
        /// <summary>启动</summary>
        /// <returns></returns>
        public override Boolean Start()
        {
            var list = Data;
            if (list == null)
            {
                // 建立消费者数据，用于心跳
                var sd = new SubscriptionData
                {
                    Topic = Topic,
                };
                var cd = new ConsumerData
                {
                    GroupName = Group,
                    SubscriptionDataSet = new[] { sd },
                };

                list = new List<ConsumerData> { cd };

                Data = list;
            }

            return base.Start();
        }
        #endregion

        #region 拉取消息
        /// <summary>从指定队列拉取消息</summary>
        /// <param name="mq"></param>
        /// <param name="offset"></param>
        /// <param name="maxNums"></param>
        /// <param name="msTimeout"></param>
        /// <returns></returns>
        public PullResult Pull(MessageQueue mq, Int64 offset, Int32 maxNums, Int32 msTimeout = -1)
        {
            var header = new PullMessageRequestHeader
            {
                ConsumerGroup = Group,
                Topic = Topic,
                QueueId = mq.QueueId,
                QueueOffset = offset,
                MaxMsgNums = maxNums,
                SysFlag = 6,
            };
            if (msTimeout >= 0) header.SuspendTimeoutMillis = msTimeout;

            var dic = header.GetProperties();
            var bk = GetBroker(mq.BrokerName);

            var rs = bk.Invoke(RequestCode.PULL_MESSAGE, null, dic, true);

            var pr = new PullResult();

            if (rs.Header.Code == 0)
                pr.Status = PullStatus.Found;
            else if (rs.Header.Code == (Int32)ResponseCode.PULL_NOT_FOUND)
                pr.Status = PullStatus.NoNewMessage;

            pr.Read(rs.Header.ExtFields);

            // 读取内容
            if (rs.Body != null && rs.Body.Length > 0) pr.Messages = MessageExt.ReadAll(rs.Body).ToArray();

            return pr;
        }
        #endregion

        #region 业务方法
        /// <summary>查询指定队列的偏移量</summary>
        /// <param name="mq"></param>
        /// <returns></returns>
        public Int64 QueryOffset(MessageQueue mq)
        {
            var bk = GetBroker(mq.BrokerName);
            var rs = bk.Invoke(RequestCode.QUERY_CONSUMER_OFFSET, null, new
            {
                consumerGroup = Group,
                topic = Topic,
                queueId = mq.QueueId,
            });

            var dic = rs.Header.ExtFields;
            if (dic == null) return -1;

            return dic["offset"].ToLong();
        }

        /// <summary>根据时间戳查询偏移</summary>
        /// <param name="mq"></param>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        public Int64 SearchOffset(MessageQueue mq, Int64 timestamp)
        {
            throw new NotImplementedException();
        }

        /// <summary>更新队列的偏移</summary>
        /// <param name="mq"></param>
        /// <param name="commitOffset"></param>
        /// <returns></returns>
        public Boolean UpdateOffset(MessageQueue mq, Int64 commitOffset)
        {
            var bk = GetBroker(mq.BrokerName);
            var rs = bk.Invoke(RequestCode.UPDATE_CONSUMER_OFFSET, null, new
            {
                consumerGroup = Group,
                topic = Topic,
                queueId = mq.QueueId,
                commitOffset,
            });

            var dic = rs.Header.ExtFields;
            if (dic == null) return false;

            return true;
        }

        /// <summary>获取消费者下所有消费者</summary>
        /// <param name="group"></param>
        public ICollection<String> GetConsumers(String group = null)
        {
            if (group.IsNullOrEmpty()) group = Group;

            var header = new
            {
                consumerGroup = group,
            };

            var cs = new HashSet<String>();

            // 在所有Broker上查询
            foreach (var item in Brokers)
            {
                try
                {
                    var bk = GetBroker(item.Name);
                    //bk.Ping();
                    var rs = bk.Invoke(RequestCode.GET_CONSUMER_LIST_BY_GROUP, null, header);
                    //WriteLog(rs.Header.ExtFields?.ToJson());
                    var js = rs.ReadBodyAsJson();
                    if (js != null && js["consumerIdList"] is IList<Object> list)
                    {
                        foreach (String clientId in list)
                        {
                            if (!cs.Contains(clientId)) cs.Add(clientId);
                        }
                    }
                }
                catch (Exception ex)
                {
                    XTrace.WriteException(ex);
                }
            }

            return cs;
        }
        #endregion

        #region 消费端负载均衡
        /// <summary>当前所需要消费的队列。由均衡算法产生</summary>
        public MessageQueue[] Queues => _Queues.Select(e => e.Queue).ToArray();

        private QueueStore[] _Queues;

        class QueueStore
        {
            public MessageQueue Queue { get; set; }
            public Int64 Offset { get; set; } = -1;
        }

        /// <summary>重新平衡消费队列</summary>
        /// <returns></returns>
        public Boolean Rebalance()
        {
            /*
             * 1，获取消费组下所有消费组，排序
             * 2，获取主题下所有队列，排序
             * 3，各消费者平均分配队列，不采用环形，减少消费者到Broker连接数
             */

            var cs = GetConsumers(Group);
            //if (cs.Count == 0)
            //{
            //    for (var i = 0; i < 60; i++)
            //    {
            //        cs = GetConsumers(Group);
            //        if (cs.Count > 0) break;

            //        Thread.Sleep(1000);
            //    }
            //}
            if (cs.Count == 0) return false;

            var qs = new List<MessageQueue>();
            foreach (var br in Brokers)
            {
                if (br.Permission.HasFlag(Permissions.Read))
                {
                    for (var i = 0; i < br.ReadQueueNums; i++)
                    {
                        qs.Add(new MessageQueue
                        {
                            Topic = Topic,
                            BrokerName = br.Name,
                            QueueId = i,
                        });
                    }
                }
            }

            // 排序，计算索引
            var cid = ClientId;
            var idx = 0;
            var cs2 = cs.OrderBy(e => e).ToList();
            for (idx = 0; idx < cs2.Count; idx++)
            {
                if (cs2[idx] == cid) break;
            }
            if (idx >= cs2.Count) return false;

            // 先分糖，每人多少个
            var ds = new Int32[cs2.Count];
            for (Int32 i = 0, k = 0; i < qs.Count; i++)
            {
                ds[k++]++;

                if (k >= ds.Length) k = 0;
            }
            // 我的前面分了多少
            var start = ds.Take(idx).Sum();
            // 跳过前面，取我的糖
            qs = qs.Skip(start).Take(ds[idx]).ToList();

            var rs = new List<QueueStore>();
            foreach (var item in qs)
            {
                rs.Add(new QueueStore { Queue = item });
            }

            _Queues = rs.ToArray();

            return true;
        }
        #endregion
    }
}