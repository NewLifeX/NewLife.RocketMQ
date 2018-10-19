using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;
using NewLife.Threading;

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

        /// <summary>拉取的批大小。默认32</summary>
        public Int32 BatchSize { get; set; } = 32;

        /// <summary>消费委托</summary>
        public Func<MessageQueue, PullResult, Boolean> OnReceive;
        #endregion

        #region 构造
        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            // 停止并保存偏移
            Stop();
            PersistAll(_Queues);

            _timer.TryDispose();
            _threads.TryDispose();
            _persist.TryDispose();
        }
        #endregion

        #region 方法
        /// <summary>启动</summary>
        /// <returns></returns>
        public override Boolean Start()
        {
            if (Active) return true;

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

            if (!base.Start()) return false;

            // 快速检查消费组，均衡成功后改为30秒一次
            _timer = new TimerX(CheckGroup, null, 100, 1_000) { Async = true };

            return true;
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
            if (rs?.Header == null) return null;

            var pr = new PullResult();

            if (rs.Header.Code == 0)
                pr.Status = PullStatus.Found;
            else if (rs.Header.Code == (Int32)ResponseCode.PULL_NOT_FOUND)
                pr.Status = PullStatus.NoNewMessage;

            pr.Read(rs.Header?.ExtFields);

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

            return dic.TryGetValue("offset", out var str) ? str.ToLong() : -1;
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
                    //XTrace.WriteException(ex);
                    WriteLog(ex.GetTrue().Message);
                }
            }

            return cs;
        }
        #endregion

        #region 消费调度
        private Thread[] _threads;
        private volatile Int32 _version;

        private void DoSchedule()
        {
            var qs = _Queues;
            if (qs == null || qs.Length == 0) return;

            _version++;

            // 关线程
            Stop();

            // 如果有多个消费者，则等一段时间让大家停止消费，尽量避免重复消费
            if (_Consumers != null && _Consumers.Length > 1) Thread.Sleep(10_000);

            // 开线程
            _threads = new Thread[qs.Length];
            for (var i = 0; i < qs.Length; i++)
            {
                var th = new Thread(DoPull)
                {
                    IsBackground = true
                };
                th.Start(qs[i]);

                _threads[i] = th;
            }

            // 定时保存偏移量
            if (_persist == null)
            {
                var time = PersistConsumerOffsetInterval;
                _persist = new TimerX(DoPersist, null, time, time) { Async = true };
            }
        }

        private void Stop()
        {
            if (_threads != null && _threads.Length > 0)
            {
                WriteLog("停止调度线程[{0}]", _threads.Length);

                // 预留一点退出时间
                foreach (var item in _threads)
                {
                    if (item.ThreadState != ThreadState.Running) continue;
                    try
                    {
                        if (item.Join(3_000)) item.Abort();
                    }
                    catch { }
                }
            }
        }

        private void DoPull(Object state)
        {
            var st = state as QueueStore;
            var mq = st.Queue;

            var v = _version;
            while (v == _version)
            {
                try
                {
                    // 查询偏移量
                    if (st.Offset < 0)
                    {
                        st.Offset = st.LastOffset = QueryOffset(mq);

                        if (st.Offset >= 0) WriteLog("开始消费[{0}@{1}] Offset={2:n0}", mq.BrokerName, mq.QueueId, st.Offset);
                    }

                    // 拉取一批，阻塞等待
                    var offset = st.Offset >= 0 ? st.Offset : 0;
                    var pr = Pull(mq, offset, BatchSize, 10_000);
                    if (pr != null && pr.Status == PullStatus.Found && pr.Messages != null && pr.Messages.Length > 0)
                    {
                        // 触发消费
                        var rs = Receive(mq, pr);

                        // 更新偏移
                        if (rs) st.Offset = pr.NextBeginOffset;
                    }
                }
                catch (ThreadAbortException) { break; }
                catch (ThreadInterruptedException) { break; }
                catch (Exception ex)
                {
                    Log?.Error(ex.GetMessage());
                }
            }

            // 保存消费进度
            if (st.Offset >= 0 && st.Offset != st.LastOffset) UpdateOffset(mq, st.Offset);
        }

        /// <summary>拉取到一批消息</summary>
        /// <param name="queue"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        protected virtual Boolean Receive(MessageQueue queue, PullResult result)
        {
            if (OnReceive != null) return OnReceive(queue, result);

            return true;
        }

        private TimerX _persist;
        private void DoPersist(Object state) { PersistAll(_Queues); }

        private void PersistAll(IEnumerable<QueueStore> stores)
        {
            if (stores == null) return;

            foreach (var item in stores)
            {
                if (item.Offset >= 0 && item.Offset != item.LastOffset)
                {
                    var mq = item.Queue;
                    WriteLog("队列[{0}@{1}]更新偏移[{2:n0}]", mq.BrokerName, mq.QueueId, item.Offset);

                    UpdateOffset(item.Queue, item.Offset);

                    item.LastOffset = item.Offset;
                }
            }
        }
        #endregion

        #region 消费端负载均衡
        /// <summary>当前所需要消费的队列。由均衡算法产生</summary>
        public MessageQueue[] Queues => _Queues.Select(e => e.Queue).ToArray();

        private QueueStore[] _Queues;
        private String[] _Consumers;

        class QueueStore
        {
            public MessageQueue Queue { get; set; }
            public Int64 Offset { get; set; } = -1;
            public Int64 LastOffset { get; set; } = -1;

            #region 相等
            /// <summary>相等比较</summary>
            /// <param name="obj"></param>
            /// <returns></returns>
            public override Boolean Equals(Object obj)
            {
                var x = this;
                if (!(obj is QueueStore y)) return false;

                return Equals(x.Queue, y.Queue) && x.Offset == y.Offset;
            }

            /// <summary>计算哈希</summary>
            /// <returns></returns>
            public override Int32 GetHashCode()
            {
                var obj = this;
                return (obj.Queue == null ? 0 : obj.Queue.GetHashCode()) ^ obj.Offset.GetHashCode();
            }
            #endregion
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

            // 如果序列相等则返回false
            var ori = _Queues;
            if (ori != null)
            {
                var q1 = ori.Select(e => e.Queue).ToArray();
                var q2 = rs.Select(e => e.Queue).ToArray();

                if (q1.SequenceEqual(q2)) return false;

                PersistAll(ori);
            }

            var dic = qs.GroupBy(e => e.BrokerName).ToDictionary(e => e.Key, e => e.Join(",", x => x.QueueId));
            WriteLog("消费重新平衡：{0}", dic.Join(";", e => $"{e.Key}[{e.Value}]"));

            _Queues = rs.ToArray();
            _Consumers = cs2.ToArray();

            return true;
        }

        private TimerX _timer;
        private void CheckGroup(Object state)
        {
            if (!Rebalance()) return;

            DoSchedule();

            _timer.Period = 30_000;
        }
        #endregion
    }
}