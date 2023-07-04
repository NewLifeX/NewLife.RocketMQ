using System.Reflection;
using System.Text;
using System.Xml.Serialization;
using NewLife.Data;
using NewLife.Log;
using NewLife.Reflection;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Models;
using NewLife.RocketMQ.Protocol;
using NewLife.RocketMQ.Protocol.ConsumerStates;
using NewLife.Serialization;
using NewLife.Threading;

namespace NewLife.RocketMQ;

/// <summary>消费者</summary>
public class Consumer : MqBase
{
    #region 属性
    /// <summary>数据</summary>
    public IList<ConsumerData> Data { get; set; }

    /// <summary>标签集合</summary>
    public String[] Tags { get; set; }

    /// <summary>消费挂起超时。每次拉取消息，服务端如果没有消息时的挂起时间，默认15_000ms</summary>
    public Int32 SuspendTimeout { get; set; } = 15_000;

    /// <summary>拉取的批大小。默认32</summary>
    public Int32 BatchSize { get; set; } = 32;

    /// <summary>启动时间</summary>
    private DateTime StartTime { get; set; } = DateTime.Now;

    /// <summary>首次消费时的消费策略，默认值false，表示从头开始收，等同于Java版的COMSUME_FROM_FIRST_OFFSET</summary>
    public Boolean FromLastOffset { get; set; } = false;

    /// <summary>
    /// 【仅FromLastOffset设置为true时生效】
    /// 跳过积压的消息数量，默认为0，即积压消息超过10000后将强制从消费最大偏移量的位置消费
    /// 若需要处理所有未消费消息，可将此值设置为0
    /// </summary>
    [Obsolete("谨慎使用该配置，该配置会破坏ConsumeFromWhere的原始意图，具体表现为（在CONSUME_FROM_LAST_OFFSET模式下）：" +
              "1.首次消费时如果队列中已有数据，但数据量未达到SkipOverStoredMsgCount设定值时，会从头部开始消费，而不是尾部；" +
              "2.非首次消费时如果队列最大偏移量与当前偏移量差值大于SkipOverStoredMsgCount时，会直接从尾部开始消费，而不是继续消费；" +
              "3.上述的两种情况都是在Consumer初始化后首次DoPull时执行的判断，也就是一般情况下与应用启动操作绑定")]
    public UInt32 SkipOverStoredMsgCount { get; set; }

    /// <summary>
    /// 订阅表达式 TAG
    /// </summary>
    public String Subscription { get; set; } = "*";

    /// <summary>消息模型。广播/集群</summary>
    public MessageModels MessageModel { get; set; } = MessageModels.Clustering;

    /// <summary>消费委托</summary>
    public Func<MessageQueue, MessageExt[], Boolean> OnConsume;

    /// <summary>异步消费委托</summary>
    public Func<MessageQueue, MessageExt[], CancellationToken, Task<Boolean>> OnConsumeAsync;

    /// <summary>消费事件</summary>
    public event EventHandler<ConsumeEventArgs> Consumed;
    #endregion

    #region 构造

    /// <summary>销毁</summary>
    /// <param name="disposing"></param>
    protected override void Dispose(Boolean disposing)
    {
        // 停止并保存偏移
        Stop();

        base.Dispose(disposing);

        _source.TryDispose();
        _source = null;

        _timer.TryDispose();
        _timer = null;
    }

    #endregion

    #region 方法

    /// <summary>启动</summary>
    /// <returns></returns>
    public override Boolean Start()
    {
        if (Active) return true;

        WriteLog("正在准备消费 {0}", Topic);

        using var span = Tracer?.NewSpan($"mq:{Topic}:Start");
        try
        {
            var list = Data;
            if (list == null)
            {
                // 建立消费者数据，用于心跳
                var sd = new SubscriptionData
                {
                    Topic = Topic,
                    TagsSet = Tags
                };
                var cd = new ConsumerData
                {
                    GroupName = Group,
                    ConsumeFromWhere = FromLastOffset ? "CONSUME_FROM_LAST_OFFSET" : "CONSUME_FROM_FIRST_OFFSET",
                    MessageModel = MessageModel.ToString().ToUpper(),
                    SubscriptionDataSet = new[] { sd },
                };

                list = new List<ConsumerData> { cd };

                Data = list;
            }

            if (!base.Start()) return false;

            // 默认自动开始调度
            if (AutoSchedule) StartSchedule();
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);

            throw;
        }

        return true;
    }

    /// <summary>
    /// 停止
    /// </summary>
    public override void Stop()
    {
        if (!Active) return;

        using var span = Tracer?.NewSpan($"mq:{Topic}:Stop");
        try
        {
            // 停止并保存偏移
            StopSchedule();
            PersistAll(_Queues);

            base.Stop();
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);

            throw;
        }
    }

    /// <summary>创建Broker客户端，已重载，设置更大的超时时间</summary>
    /// <param name="name"></param>
    /// <param name="addrs"></param>
    /// <returns></returns>
    protected override BrokerClient CreateBroker(String name, String[] addrs)
    {
        var client = base.CreateBroker(name, addrs);
        if (client.Timeout < SuspendTimeout) client.Timeout = SuspendTimeout;

        return client;
    }
    #endregion

    #region 拉取消息

    /// <summary>从指定队列拉取消息</summary>
    /// <param name="mq"></param>
    /// <param name="offset"></param>
    /// <param name="maxNums"></param>
    /// <param name="msTimeout"></param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<PullResult> Pull(MessageQueue mq, Int64 offset, Int32 maxNums, Int32 msTimeout = -1, CancellationToken cancellationToken = default)
    {
        var header = new PullMessageRequestHeader
        {
            ConsumerGroup = Group,
            Topic = Topic,
            Subscription = Subscription,
            QueueId = mq.QueueId,
            QueueOffset = offset,
            MaxMsgNums = maxNums,
            SysFlag = 6,
            SubVersion = StartTime.ToLong(),
        };
        if (msTimeout >= 0) header.SuspendTimeoutMillis = msTimeout;

        var st = _Queues.FirstOrDefault(e => e.Queue == mq);
        if (st != null) header.CommitOffset = st.CommitOffset;

        var dic = header.GetProperties();
        var bk = GetBroker(mq.BrokerName);

        var rs = await bk.InvokeAsync(RequestCode.PULL_MESSAGE, null, dic, true, cancellationToken);
        if (rs?.Header == null) return null;

        var pr = new PullResult();

        if (rs.Header.Code == 0)
            pr.Status = PullStatus.Found;
        else if (rs.Header.Code == (Int32)ResponseCode.PULL_NOT_FOUND)
            pr.Status = PullStatus.NoNewMessage;
        else if (rs.Header.Code == (Int32)ResponseCode.PULL_OFFSET_MOVED || rs.Header.Code == (Int32)ResponseCode.PULL_RETRY_IMMEDIATELY)
            pr.Status = PullStatus.OffsetIllegal;
        else
        {
            pr.Status = PullStatus.Unknown;
            Log.Warn("响应编号：{0} 响应备注：{1} 序列编号：{2} 序列偏移量：{3}", rs.Header.Code, rs.Header.Remark, mq.QueueId, offset);
        }

        pr.Read(rs.Header?.ExtFields);

        // 读取内容
        var pk = rs.Payload;
        if (pk != null) pr.Messages = MessageExt.ReadAll(pk).ToArray();

        return pr;
    }

    #endregion

    #region 业务方法

    /// <summary>查询指定队列的偏移量</summary>
    /// <param name="mq"></param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<Int64> QueryOffset(MessageQueue mq, CancellationToken cancellationToken = default)
    {
        var bk = GetBroker(mq.BrokerName);
        var rs = await bk.InvokeAsync(RequestCode.QUERY_CONSUMER_OFFSET, null, new
        {
            consumerGroup = Group,
            topic = Topic,
            queueId = mq.QueueId,
        }, true, cancellationToken);

        var dic = rs.Header?.ExtFields;
        if (dic == null) return -1;

        return dic.TryGetValue("offset", out var str) ? str.ToLong() : -1;
    }

    /// <summary>
    /// 查询“队列”最大偏移量，不是消费提交的最后偏移量
    /// </summary>
    /// <param name="mq"></param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<Int64> QueryMaxOffset(MessageQueue mq, CancellationToken cancellationToken = default)
    {
        var bk = GetBroker(mq.BrokerName);
        var rs = await bk.InvokeAsync(RequestCode.GET_MAX_OFFSET, null, new
        {
            consumerGroup = Group,
            topic = Topic,
            queueId = mq.QueueId,
        }, true, cancellationToken);

        var dic = rs.Header?.ExtFields;
        if (dic == null) return -1;

        return dic.TryGetValue("offset", out var str) ? str.ToLong() : -1;
    }

    /// <summary>
    /// 获取最小偏移量
    /// </summary>
    /// <param name="mq"></param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<Int64> QueryMinOffset(MessageQueue mq, CancellationToken cancellationToken = default)
    {
        var bk = GetBroker(mq.BrokerName);
        var rs = await bk.InvokeAsync(RequestCode.GET_MIN_OFFSET, null, new
        {
            consumerGroup = Group,
            topic = Topic,
            queueId = mq.QueueId,
        }, true, cancellationToken);

        var dic = rs.Header?.ExtFields;
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
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<Boolean> UpdateOffset(MessageQueue mq, Int64 commitOffset, CancellationToken cancellationToken = default)
    {
        var bk = GetBroker(mq.BrokerName);
        var rs = await bk.InvokeAsync(RequestCode.UPDATE_CONSUMER_OFFSET, null, new
        {
            commitOffset,
            consumerGroup = Group,
            queueId = mq.QueueId,
            topic = Topic,
        }, false, cancellationToken);

        var dic = rs?.Header?.ExtFields;
        if (dic == null) return false;

        return true;
    }

    /// <summary>获取消费者下所有消费者</summary>
    /// <param name="group"></param>
    public ICollection<String> GetConsumers(String group = null)
    {
        if (group.IsNullOrEmpty()) group = Group;

        var header = new { consumerGroup = group, };

        var cs = new HashSet<String>();

        // 在所有Broker上查询
        foreach (var item in Brokers)
        {
            using var span = Tracer?.NewSpan($"mq:{Topic}:GetConsumers", item.Name);
            try
            {
                var bk = GetBroker(item.Name);
                //bk.Ping();
                var rs = bk.Invoke(RequestCode.GET_CONSUMER_LIST_BY_GROUP, null, header);
                //WriteLog(rs.Header.ExtFields?.ToJson());
                var js = rs.ReadBodyAsJson();
                span?.AppendTag(js);
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
                if (ex is not ResponseException)
                    span?.SetError(ex, null);

                //XTrace.WriteException(ex);
                WriteLog(ex.GetTrue().Message);
            }
        }

        return cs;
    }

    #endregion

    #region 消费调度

    private Task[] _tasks;
    private volatile Int32 _version;
    private CancellationTokenSource _source;

    /// <summary>启动消费者时自动开始调度。默认true</summary>
    public Boolean AutoSchedule { get; set; } = true;

    /// <summary>开始调度</summary>
    public void StartSchedule()
    {
        if (_timer != null) return;
        lock (this)
        {
            if (_timer != null) return;

            // 快速检查消费组，均衡成功后改为30秒一次
            _timer = new TimerX(CheckGroup, null, 100, 1_000) { Async = true };
        }
    }

    private void DoSchedule()
    {
        var qs = _Queues;
        if (qs == null || qs.Length == 0) return;

        Interlocked.Increment(ref _version);

        // 关线程
        Stop();

        // 如果有多个消费者，则等一段时间让大家停止消费，尽量避免重复消费
        //if (_Consumers != null && _Consumers.Length > 1) Thread.Sleep(10_000);

        // 释放资源
        if (_source != null)
        {
            _source.Cancel();
            _source.TryDispose();
        }

        var source = new CancellationTokenSource();

        // 开线程
        var tasks = new Task[qs.Length];
        for (var i = 0; i < qs.Length; i++)
        {
            var queueStore = qs[i];
            var task = Task.Run(async () =>
            {
                await DoPull(queueStore, source.Token);
            });
            tasks[i] = task;
        }
        _tasks = tasks;

        _source = source;
    }

    /// <summary>停止</summary>
    public void StopSchedule()
    {
        var ts = _tasks;
        if (ts != null && ts.Length > 0)
        {
            WriteLog("停止调度线程[{0}]", ts.Length);

            // 预留一点退出时间
            Interlocked.Increment(ref _version);

            // 释放资源
            if (_source != null)
            {
                _source.Cancel();
                _source.TryDispose();
                _source = null;
            }

            var timeout = TimeSpan.FromSeconds(3 * _tasks.Length);
            try
            {
                Task.WaitAll(_tasks, timeout);
            }
            catch
            {
                // 理论上不会遇到异常
                // 但等待过程可能会遇到积压的 Task 异常，统统吃掉，从业务上也没有需要捕获的需要
            }

            _tasks = null;
        }
    }

    private async Task DoPull(QueueStore st, CancellationToken cancellationToken)
    {
        var mq = st.Queue;

        var currentVersion = _version;
        while (currentVersion == _version && !cancellationToken.IsCancellationRequested)
        {
            DefaultSpan.Current = null;
            try
            {
                var offset = st.Offset;
                var pr = await Pull(mq, offset, BatchSize, SuspendTimeout, cancellationToken);
                if (pr != null)
                {
                    switch (pr.Status)
                    {
                        case PullStatus.Found:
                            if (pr.Messages != null && pr.Messages.Length > 0)
                            {
                                DefaultSpan.Current = null;

                                // 性能埋点
                                using var span = Tracer?.NewSpan($"mq:{Topic}:Consume", pr.Messages);
                                try
                                {
                                    // 触发消费
                                    var rs = await Consume(mq, pr, cancellationToken);

                                    // 更新偏移
                                    if (rs)
                                    {
                                        st.Offset = pr.NextBeginOffset;
                                        // 提交消费进度
                                        await UpdateOffset(mq, st.Offset, cancellationToken);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    span?.SetError(ex, mq);

                                    throw;
                                }
                            }

                            break;
                        case PullStatus.NoNewMessage:
                            break;
                        case PullStatus.NoMatchedMessage:
                            break;
                        case PullStatus.OffsetIllegal:
                            if (pr.NextBeginOffset >= 0)
                            {
                                WriteLog("无效的offset，可能历史消息已过期 [{0}@{1}] Offset={2:n0}, NextOffset={3:n0}", mq.BrokerName, mq.QueueId, st.Offset, pr.NextBeginOffset);
                                st.Offset = pr.NextBeginOffset;
                            }

                            break;
                        case PullStatus.Unknown:
                            Log.Error("未知响应类型消息序列[{1}]偏移量{0}", st.Offset, st.Queue.QueueId);
                            break;
                        default:
                            break;
                    }
                }
            }
            catch (ThreadAbortException) { break; }
            catch (ThreadInterruptedException) { break; }
            catch (TaskCanceledException) { }
            catch (AggregateException) { }
            catch (Exception ex)
            {
                Log?.Error(ex.GetMessage());
                // 出现其他异常的情况下，等待一会，防止出现大量异常
                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }

        // 保存消费进度
        if (st.Offset >= 0 && st.Offset != st.CommitOffset) await UpdateOffset(mq, st.Offset, cancellationToken);
    }

    /// <summary>拉取到一批消息</summary>
    /// <param name="queue"></param>
    /// <param name="result"></param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    protected virtual async Task<Boolean> Consume(MessageQueue queue, PullResult result, CancellationToken cancellationToken)
    {
        if (Log != null && Log.Level <= LogLevel.Debug) WriteLog("{0}", result);

        Consumed?.Invoke(this, new ConsumeEventArgs { Queue = queue, Messages = result.Messages, Result = result });

        if (OnConsume != null) return OnConsume(queue, result.Messages);
        if (OnConsumeAsync != null) return await OnConsumeAsync(queue, result.Messages, cancellationToken);

        return true;
    }

    private void PersistAll(IEnumerable<QueueStore> stores)
    {
        if (stores == null) return;

        var ts = new List<Task>();
        using var source = new CancellationTokenSource(5_000);

        foreach (var item in stores)
        {
            if (item.Offset >= 0 && item.Offset != item.CommitOffset)
            {
                var mq = item.Queue;
                WriteLog("队列[{0}@{1}]更新偏移[{2:n0}]", mq.BrokerName, mq.QueueId, item.Offset);

                ts.Add(UpdateOffset(item.Queue, item.Offset, source.Token));

                item.CommitOffset = item.Offset;
            }
        }

        Task.WhenAll(ts);
    }

    #endregion

    #region 消费端负载均衡

    /// <summary>当前所需要消费的队列。由均衡算法产生</summary>
    public MessageQueue[] Queues => _Queues?.Select(e => e.Queue).ToArray();

    private QueueStore[] _Queues;
    //private String[] _Consumers;

    class QueueStore
    {
        [XmlIgnore] public MessageQueue Queue { get; set; }
        public Int64 Offset { get; set; } = -1;
        public Int64 CommitOffset { get; set; } = -1;

        #region 相等

        /// <summary>相等比较</summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override Boolean Equals(Object obj) => obj is QueueStore y && Equals(Queue, y.Queue) && Offset == y.Offset;

        /// <summary>计算哈希</summary>
        /// <returns></returns>
        public override Int32 GetHashCode() => (Queue == null ? 0 : Queue.GetHashCode()) ^ Offset.GetHashCode();

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

        if (_Queues == null) WriteLog("准备从所有Broker服务器上获取消费者列表，以确定当前消费者应该负责消费的queue分片");

        var cs = GetConsumers(Group);
        if (cs.Count == 0) return false;

        var qs = new List<MessageQueue>();
        foreach (var br in Brokers)
        {
            if (br.Permission.HasFlag(Permissions.Read))
            {
                for (var i = 0; i < br.ReadQueueNums; i++)
                {
                    qs.Add(new MessageQueue { Topic = Topic, BrokerName = br.Name, QueueId = i, });
                }
            }
        }

        var cs2 = cs.OrderBy(e => e).ToList();

        if (_Queues == null) WriteLog("消费者列表[{0}]：{1}", cs2.Count, cs2.Join());

        // 集群模式需要分配Queue，而广播模式不需要
        if (MessageModel == MessageModels.Clustering)
        {
            // 排序，计算索引。如果当前节点不在消费者列表里，则跳过
            var cid = ClientId;
            var idx = cs2.IndexOf(cid);
            if (idx < 0 || idx >= cs2.Count) return false;

            // 先分糖，每人多少个。你一个我一个，一圈又一圈的分。
            // 如果无法均分，前面的消费者会比后面的消费者多拿一个，并且最多只会多一个
            var ds = new Int32[cs2.Count];
            for (Int32 i = 0, k = 0; i < qs.Count; i++)
            {
                ds[k++]++;

                if (k >= ds.Length) k = 0;
            }

            // 我的前面分了多少，就是前面的桶内数字之和
            var start = ds.Take(idx).Sum();
            // 跳过前面，取我的糖
            qs = qs.Skip(start).Take(ds[idx]).ToList();
        }

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
        var str = dic.Join(";", e => $"{e.Key}[{e.Value}]");
        WriteLog("消费重新平衡，当前消费者负责queue分片：{0}", str);

        using var span = Tracer?.NewSpan($"mq:{Topic}:Rebalance", str);

        _Queues = rs.ToArray();
        InitOffsetAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        //_Consumers = cs2.ToArray();

        return true;
    }

    private TimerX _timer;
    private DateTime _nextCheck;
    private Boolean _checking;

    /// <summary>检查消费组，如果消费者有变化，则需要重新平衡，重新分配各消费者所处理的队列</summary>
    /// <param name="state"></param>
    private void CheckGroup(Object state = null)
    {
        if (_checking) return;

        // 避免多次平衡同时进行
        var now = TimerX.Now;
        if (now < _nextCheck) return;

        lock (this)
        {
            if (_checking) return;
            _checking = true;

            using var span = Tracer?.NewSpan($"mq:{Topic}:CheckGroup");
            try
            {
                if (!Rebalance()) return;

                if (AutoSchedule) DoSchedule();

                if (_timer != null) _timer.Period = 60_000;
                _nextCheck = now.AddSeconds(3);
            }
            catch (Exception ex)
            {
                span?.SetError(ex, null);

                XTrace.WriteException(ex);
            }
            finally
            {
                _checking = false;
            }
        }
    }

    private async Task InitOffsetAsync(CancellationToken cancellationToken = default)
    {
        if (_Queues == null || _Queues.Length == 0) return;

        var offsetTables = new Dictionary<MessageQueueModel, OffsetWrapperModel>();
        var queueBrokers = _Queues.Select(t => t.Queue.BrokerName).Distinct().ToList();//获取当前消费这分配到的服务器及服务器队列
        foreach (var brokerName in queueBrokers)
        {
            var broker = GetBroker(brokerName);
            var command = await broker.InvokeAsync(RequestCode.GET_CONSUME_STATS, null, new
            {
                consumerGroup = Group,
                topic = Topic
            }, true, cancellationToken);
            var consumerStates = ConsumerStatesSpecialJsonHandler(command.Payload);
            //foreach (var (key, value) in consumerStates.OffsetTable) offsetTables.Add(key, value);
            foreach (var item in consumerStates.OffsetTable)
            {
                offsetTables.Add(item.Key, item.Value);
            }
        }

        var neverConsumed = offsetTables.All(t => t.Value.LastTimestamp == 0); //表示没消费过

        foreach (var store in _Queues)
        {
            if (store.Offset >= 0) continue;

            var item = offsetTables.FirstOrDefault(t => t.Key.BrokerName == store.Queue.BrokerName && t.Key.QueueId == store.Queue.QueueId);
            var offsetTable = item.Value;
#pragma warning disable CS0618 // 类型或成员已过时
            var skipOver = SkipOverStoredMsgCount;
#pragma warning restore CS0618 // 类型或成员已过时
            if (neverConsumed)
            {
                var maxOffset = offsetTable.BrokerOffset;
                var offset = FromLastOffset ? maxOffset : 0L;
                /*
                 * 下面这个判断是专门为SkipOverStoredMsgCount设置的，根据SkipOverStoredMsgCount，
                 * 根据SkipOverStoredMsgCount的原始定义，只有在积压量超过了SkipOverStoredMsgCount
                 * 设定值才会遵从FromLastOffset规则，在没有达到SkipOverStoredMsgCount设定值还是会
                 * 从头开始消费，以后在确定删除SkipOverStoredMsgCount时直接删除下面if代码段即可
                 */
                if (FromLastOffset && skipOver > 0 && maxOffset < skipOver)
                {
                    offset = 0L;
                }

                store.Offset = store.CommitOffset = offset;
                await UpdateOffset(store.Queue, offset, cancellationToken);
            }
            else
            {
                // var offset = await QueryOffset(store.Queue);
                var offset = offsetTable.ConsumerOffset;
                /*
                 * 下面这个判断是专门为SkipOverStoredMsgCount设置的，根据SkipOverStoredMsgCount，
                 * 根据SkipOverStoredMsgCount的原始定义，在当前积压量大于SkipOverStoredMsgCount
                 * 设定值时，直接从最大偏移量开始消费，以后在确定删除SkipOverStoredMsgCount时
                 * 直接删除下面if代码段即可
                 */
                if (FromLastOffset && skipOver > 0 && offset + skipOver <= offsetTable.BrokerOffset)
                {
                    offset = offsetTable.BrokerOffset;
                }

                store.Offset = store.CommitOffset = offset;
            }

            WriteLog("初始化offset[{0}@{1}] Offset={2:n0}", store.Queue.BrokerName, store.Queue.QueueId, store.Offset);
        }
    }

    /// <summary>
    /// 消费者状态信息特殊Json处理
    /// </summary>
    /// <param name="payload">负载数据</param>
    /// <returns></returns>
    private ConsumerStatesModel ConsumerStatesSpecialJsonHandler(Packet payload)
    {
        #region <cmd formate/>

        // Apache RocketMQ 获取Consumer_States返回结果
        // 返回消息格式不是正常的JSON需要特殊处理
        // {
        //     "consumeTps":0.0,
        //     "offsetTable":{
        //         {"brokerName":"wh-sr-11-26","queueId":0,"topic":"mip_topic_0"}:{"brokerOffset":5,"consumerOffset":35,"lastTimestamp":0},
        //         {"brokerName":"wh-sr-11-26","queueId":1,"topic":"mip_topic_0"}:{"brokerOffset":5,"consumerOffset":35,"lastTimestamp":0}
        //     }
        // }

        // 阿里版本 RocketMQ 获取Consumer_States返回结果
        // 返回消息格式不是正常的JSON需要特殊处理
        // {
        //     "consumeTps":0.0,
        //     "offsetTable":{
        //         { "brokerName":"cn-qingdao-public-share-05-2","mainQueue":false,"queueGroupId":-1,"queueId":5,"topic":"mip_topic_0"}:
        //         { "brokerOffset":4,"consumerOffset":4,"earliestUnPulledTimestamp":0,"earliestUnconsumedTimestamp":0,
        //         "inFlightMsgCountEstimatedAccumulation":0,"lagEstimatedAccumulation":0,"lastTimestamp":1647746454641,"pullOffset":4}
        //     }
        // }

        #endregion

        var cmdStr = payload.ToStr();
        cmdStr = cmdStr[1..^1];

        var indexOf = cmdStr.IndexOf('{') + 1;
        var lastIndexOf = cmdStr.LastIndexOf('}');
        cmdStr = cmdStr[indexOf..lastIndexOf];

        var offsetArr = cmdStr.Split('}');
        var offsetNew = (from offset in offsetArr
                         where !String.IsNullOrWhiteSpace(offset)
                         select String.Concat(offset.Trim(',').Trim(':'), "}")).ToList();

        var consumerStatesModel = new ConsumerStatesModel() { OffsetTable = new Dictionary<MessageQueueModel, OffsetWrapperModel>() };

        for (var i = 0; i < offsetNew.Count / 2; i++)
        {
            var list = offsetNew.Skip(i * 2).Take(2).ToList();

            var messageQueue = list[0].ToJsonEntity<MessageQueueModel>();
            var offsetWrapper = list[1].ToJsonEntity<OffsetWrapperModel>();
            consumerStatesModel.OffsetTable.Add(messageQueue, offsetWrapper);
        }

        return consumerStatesModel;
    }

    #endregion

    #region 下行指令

    /// <summary>收到命令</summary>
    /// <param name="cmd"></param>
    protected override Command OnReceive(Command cmd)
    {
        if (!cmd.Reply)
        {
            var code = (RequestCode)cmd.Header.Code;
            switch (code)
            {
                case RequestCode.NOTIFY_CONSUMER_IDS_CHANGED:
                    NotifyConsumerIdsChanged(cmd);
                    break;
                case RequestCode.RESET_CONSUMER_CLIENT_OFFSET:
                    ResetOffset(cmd);
                    break;
                case RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT:
                    GetConsumeStatus(cmd);
                    break;
                case RequestCode.GET_CONSUMER_RUNNING_INFO:
                    return GetConsumerRunningInfo(cmd);
                default:
                    break;
            }
        }

        return null;
    }

    private void NotifyConsumerIdsChanged(Command cmd) => _timer?.SetNext(-1);

    private void ResetOffset(Command cmd)
    {
        var js = cmd.Payload?.ToStr();
        if (js.IsNullOrEmpty()) return;

        // 请求内容是一个奇怪的Json，Key是MessageQueue对象，Value是偏移量
        var ss = js.Split(",{");
        foreach (var item in ss)
        {
            var name = item.Substring("\"brokerName\":", ",").Trim('\"');
            var qid = item.Substring("\"queueId\":", ",").ToInt();
            var offset = item.TrimEnd('}').Substring("}:", null).ToLong();

            var mq = _Queues.FirstOrDefault(e => e.Queue.BrokerName == name & e.Queue.QueueId == qid);
            if (mq != null) mq.Offset = offset;
        }
    }

    private void GetConsumeStatus(Command cmd) { }

    private Command GetConsumerRunningInfo(Command cmd)
    {
        var ci = new ConsumerRunningInfo();
        var dic = new Dictionary<String, String>();
        foreach (var pi in GetType().GetProperties())
        {
            if (pi.DeclaringType == typeof(DisposeBase)) continue;
            if (pi.PropertyType.GetTypeCode() == TypeCode.Object) continue;

            var val = pi.GetValue(this, null);
            if (val != null) dic[pi.Name] = val + "";
        }

        var asm = Assembly.GetExecutingAssembly();
        dic["PROP_CLIENT_VERSION"] = asm.GetName().Version + "";
        dic["PROP_CONSUMEORDERLY"] = "false";
        dic["PROP_CONSUMER_START_TIMESTAMP"] = StartTime.ToInt() + "";
        dic["PROP_CONSUME_TYPE"] = "CONSUME_PASSIVELY";
        dic["PROP_NAMESERVER_ADDR"] = NameServerAddress;
        dic["PROP_THREADPOOL_CORE_SIZE"] = (_tasks?.Length ?? 1).ToString();
        dic["messageModel"] = "CLUSTERING";
        ci.Properties = dic;

        var sd = new SubscriptionData { Topic = Topic, };
        ci.SubscriptionSet = new[] { sd };

        var sb = new StringBuilder();
        sb.Append('{');
        {
            sb.Append("\"mqTable\":{");
            for (var i = 0; i < _Queues.Length; i++)
            {
                if (i > 0) sb.Append(',');

                var item = _Queues[i];

                sb.Append(JsonWriter.ToJson(item.Queue, false, false, true));
                sb.Append(':');
                sb.Append(JsonWriter.ToJson(item, false, false, true));
            }

            sb.Append('}');
        }
        {
            sb.Append(',');
            sb.Append("\"properties\":");
            sb.Append(ci.Properties.ToJson());
        }
        {
            sb.Append(',');
            sb.Append("\"subscriptionSet\":");
            sb.Append(JsonWriter.ToJson(ci.SubscriptionSet, false, false, true));
        }
        sb.Append('}');

        var rs = cmd.CreateReply() as Command;
        rs.Header.Language = "DOTNET";
        rs.Payload = sb.ToString().GetBytes();

        return rs;
    }

    #endregion
}