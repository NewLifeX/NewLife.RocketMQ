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

using NewLife.RocketMQ.MessageTrace;

namespace NewLife.RocketMQ;

/// <summary>消费者</summary>
public class Consumer : MqBase
{
    #region 属性
    /// <summary>数据</summary>
    public IList<ConsumerData> Data { get; set; }

    /// <summary>标签集合</summary>
    public String[] Tags { get; set; }

    /// <summary>多主题订阅列表。设置后一个Consumer可同时消费多个Topic，每个Topic使用相同的Tags和ExpressionType</summary>
    /// <remarks>
    /// 设置 Topics 后，原 Topic 属性作为默认主题，Topics 中的所有主题都会被订阅。
    /// 如果 Topics 未设置，则保持原有单 Topic 行为不变。
    /// </remarks>
    public String[] Topics { get; set; }

    /// <summary>消费挂起超时。每次拉取消息，服务端如果没有消息时的挂起时间，默认15_000ms</summary>
    public Int32 SuspendTimeout { get; set; } = 15_000;

    /// <summary>拉取的批大小。默认32</summary>
    public Int32 BatchSize { get; set; } = 32;

    /// <summary>启动时间</summary>
    private DateTime StartTime { get; set; } = DateTime.Now;

    /// <summary>首次消费时的消费策略，默认值false，表示从头开始收，等同于Java版的COMSUME_FROM_FIRST_OFFSET</summary>
    public Boolean FromLastOffset { get; set; } = false;

    /// <summary>
    /// 订阅表达式 TAG
    /// </summary>
    public String Subscription { get; set; } = "*";

    /// <summary>表达式类型。TAG或SQL92，默认TAG。使用SQL92时Subscription填写SQL表达式</summary>
    public String ExpressionType { get; set; } = "TAG";

    /// <summary>启动消费者时自动开始调度。默认true</summary>
    public Boolean AutoSchedule { get; set; } = true;

    /// <summary>消息模型。广播/集群</summary>
    public MessageModels MessageModel { get; set; } = MessageModels.Clustering;

    /// <summary>消费类型。CONSUME_PASSIVELY/CONSUME_ACTIVELY</summary>
    public String ConsumeType { get; set; } = "CONSUME_PASSIVELY";

    /// <summary>最大重试次数。默认16次，超过后进入死信队列</summary>
    public Int32 MaxReconsumeTimes { get; set; } = 16;

    /// <summary>是否启用消费重试。默认true，消费失败时自动将消息发回Broker的RETRY Topic</summary>
    public Boolean EnableRetry { get; set; } = true;

    /// <summary>重试延迟等级。默认0表示由Broker根据重试次数决定延迟，大于0时使用指定等级</summary>
    public Int32 RetryDelayLevel { get; set; }

    /// <summary>是否顺序消费。启用后消费前自动锁定队列，确保同一时刻只有一个消费者</summary>
    public Boolean OrderConsume { get; set; }

    /// <summary>最大并发消费数。0表示不限制，每个队列一个消费线程。大于0时使用信号量控制所有队列的总并发</summary>
    public Int32 MaxConcurrentConsume { get; set; }

    /// <summary>消费委托</summary>
    public Func<MessageQueue, MessageExt[], Boolean> OnConsume;

    /// <summary>异步消费委托</summary>
    public Func<MessageQueue, MessageExt[], CancellationToken, Task<Boolean>> OnConsumeAsync;

    /// <summary>消费事件</summary>
    public event EventHandler<ConsumeEventArgs> Consumed;

    private readonly IList<IConsumeMessageHook> _consumeMessageHooks = new List<IConsumeMessageHook>();
    private AsyncTraceDispatcher _traceDispatcher;

    /// <summary>本地偏移存储路径。广播模式时使用，默认当前目录下的 .offsets/{Group}.json</summary>
    public String OffsetStorePath { get; set; }

    private SemaphoreSlim _concurrentSemaphore;

    /// <summary>获取所有有效的订阅主题</summary>
    private String[] GetEffectiveTopics()
    {
        var topics = Topics;
        if (topics != null && topics.Length > 0) return topics;
        return [Topic];
    }
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
    protected override void OnStart()
    {
        var allTopics = GetEffectiveTopics();
        WriteLog("正在准备消费 {0}", allTopics.Join(","));

        if (EnableMessageTrace)
        {
            _traceDispatcher = new AsyncTraceDispatcher();
            _traceDispatcher.Start(NameServerAddress);
            _consumeMessageHooks.Add(new MessageTraceHook(_traceDispatcher));
        }

        var list = Data;
        if (list == null)
        {
            // 为每个Topic建立订阅数据
            var sds = allTopics.Select(t => new SubscriptionData
            {
                Topic = t,
                TagsSet = Tags
            }).ToArray();

            var cd = new ConsumerData
            {
                GroupName = Group,
                ConsumeFromWhere = FromLastOffset ? "CONSUME_FROM_LAST_OFFSET" : "CONSUME_FROM_FIRST_OFFSET",
                MessageModel = MessageModel.ToString().ToUpper(),
                SubscriptionDataSet = sds,
                ConsumeType = ConsumeType,
            };

            list = new[] { cd };

            Data = list;
        }

        base.OnStart();

        // 多Topic时，通知NameClient轮询额外主题的路由
        if (allTopics.Length > 1 && _NameServer != null)
        {
            var extras = allTopics.Where(t => t != Topic).ToArray();
            _NameServer.ExtraTopics = extras;

            // 首次获取额外主题的路由
            foreach (var topic in extras)
            {
                try
                {
                    _NameServer.GetRouteInfo(topic);
                }
                catch (Exception ex)
                {
                    WriteLog("获取主题[{0}]路由失败：{1}", topic, ex.Message);
                }
            }
        }

        // 默认自动开始调度
        if (AutoSchedule) StartSchedule();
    }

    /// <summary>
    /// 停止
    /// </summary>
    protected override void OnStop()
    {
        // 停止并保存偏移
        StopSchedule();
        PersistAll(_Queues).Wait();

        base.OnStop();
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
            Topic = mq.Topic ?? Topic,
            Subscription = Subscription,
            ExpressionType = ExpressionType,
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

        var rs = await bk.InvokeAsync(RequestCode.PULL_MESSAGE, null, dic, true, cancellationToken).ConfigureAwait(false);
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
            Log.Warn("[{0}]{1} 序列编号：{2} 序列偏移量：{3}", (ResponseCode)rs.Header.Code, rs.Header.Remark, mq.QueueId, offset);
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
            topic = mq.Topic ?? Topic,
            queueId = mq.QueueId,
        }, true, cancellationToken).ConfigureAwait(false);

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
            topic = mq.Topic ?? Topic,
            queueId = mq.QueueId,
        }, true, cancellationToken).ConfigureAwait(false);

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
            topic = mq.Topic ?? Topic,
            queueId = mq.QueueId,
        }, true, cancellationToken).ConfigureAwait(false);

        var dic = rs.Header?.ExtFields;
        if (dic == null) return -1;

        return dic.TryGetValue("offset", out var str) ? str.ToLong() : -1;
    }

    /// <summary>根据时间戳查询偏移</summary>
    /// <param name="mq">队列</param>
    /// <param name="timestamp">时间戳（毫秒）</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<Int64> SearchOffset(MessageQueue mq, Int64 timestamp, CancellationToken cancellationToken = default)
    {
        var bk = GetBroker(mq.BrokerName);
        var rs = await bk.InvokeAsync(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, null, new
        {
            topic = mq.Topic ?? Topic,
            queueId = mq.QueueId,
            timestamp,
        }, true, cancellationToken).ConfigureAwait(false);

        var dic = rs.Header?.ExtFields;
        if (dic == null) return -1;

        return dic.TryGetValue("offset", out var str) ? str.ToLong() : -1;
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
            topic = mq.Topic ?? Topic,
        }, false, cancellationToken).ConfigureAwait(false);

        //var dic = rs?.Header?.ExtFields;
        //if (dic == null) return false;

        return true;
    }

    /// <summary>获取消费者下所有消费者</summary>
    /// <param name="group"></param>
    public async Task<ICollection<String>> GetConsumers(String group = null)
    {
        if (group.IsNullOrEmpty()) group = Group;

        var header = new { consumerGroup = group, };

        var cs = new HashSet<String>();

        // 在所有Broker上查询
        foreach (var item in Brokers)
        {
            using var span = Tracer?.NewSpan($"mq:{Name}:GetConsumers", item.Name);
            try
            {
                var bk = GetBroker(item.Name);
                //bk.Ping();
                var rs = await bk.InvokeAsync(RequestCode.GET_CONSUMER_LIST_BY_GROUP, null, header).ConfigureAwait(false);
                span?.AppendTag(rs.Payload?.ToStr());
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
                if (ex is not ResponseException)
                    span?.SetError(ex, null);

                //XTrace.WriteException(ex);
                WriteLog(ex.GetTrue().Message);
            }
        }

        return cs;
    }

    /// <summary>获取消费者连接列表</summary>
    /// <param name="group">消费组名</param>
    /// <returns></returns>
    public async Task<IDictionary<String, Object>> GetConsumerConnectionList(String group = null)
    {
        if (group.IsNullOrEmpty()) group = Group;

        foreach (var item in Brokers)
        {
            using var span = Tracer?.NewSpan($"mq:{Name}:GetConsumerConnectionList", item.Name);
            try
            {
                var bk = GetBroker(item.Name);
                var rs = await bk.InvokeAsync(RequestCode.GET_CONSUMER_CONNECTION_LIST, null, new { consumerGroup = group }).ConfigureAwait(false);
                if (rs?.Payload != null) return rs.ReadBodyAsJson();
            }
            catch (Exception ex)
            {
                span?.SetError(ex, null);
            }
        }

        return null;
    }

    /// <summary>重置消费偏移</summary>
    /// <param name="timestamp">时间戳（毫秒），将偏移重置到该时间点</param>
    /// <param name="group">消费组名</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<Boolean> ResetConsumerOffset(Int64 timestamp, String group = null, CancellationToken cancellationToken = default)
    {
        if (group.IsNullOrEmpty()) group = Group;

        using var span = Tracer?.NewSpan($"mq:{Name}:ResetConsumerOffset", timestamp);
        try
        {
            foreach (var item in Brokers)
            {
                var bk = GetBroker(item.Name);
                await bk.InvokeAsync(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, null, new
                {
                    topic = Topic,
                    group,
                    timestamp,
                    isForce = false,
                }, true, cancellationToken).ConfigureAwait(false);
            }

            return true;
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            WriteLog("重置消费偏移失败：{0}", ex.Message);
            return false;
        }
    }

    #endregion

    #region 顺序消费锁定
    /// <summary>批量锁定队列。顺序消费时确保同一队列同一时刻只有一个消费者</summary>
    /// <param name="mqs">待锁定的队列集合</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>成功锁定的队列集合</returns>
    public async Task<IList<MessageQueue>> LockBatchMQAsync(IList<MessageQueue> mqs, CancellationToken cancellationToken = default)
    {
        if (mqs == null || mqs.Count == 0) return [];

        var locked = new List<MessageQueue>();
        var groups = mqs.GroupBy(e => e.BrokerName);

        foreach (var g in groups)
        {
            using var span = Tracer?.NewSpan($"mq:{Name}:LockBatchMQ", g.Key);
            try
            {
                var bk = GetBroker(g.Key);
                if (bk == null) continue;

                var body = new
                {
                    consumerGroup = Group,
                    clientId = ClientId,
                    mqSet = g.Select(e => new { topic = e.Topic, brokerName = e.BrokerName, queueId = e.QueueId }).ToArray(),
                };

                var rs = await bk.InvokeAsync(RequestCode.LOCK_BATCH_MQ, body, null, true, cancellationToken).ConfigureAwait(false);
                if (rs?.Payload != null)
                {
                    var js = rs.ReadBodyAsJson();
                    if (js != null && js["lockOKMQSet"] is IList<Object> list)
                    {
                        foreach (IDictionary<String, Object> item in list)
                        {
                            locked.Add(new MessageQueue
                            {
                                Topic = item["topic"] + "",
                                BrokerName = item["brokerName"] + "",
                                QueueId = item["queueId"].ToInt(),
                            });
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                span?.SetError(ex, null);
                WriteLog("锁定队列失败[{0}]：{1}", g.Key, ex.Message);
            }
        }

        return locked;
    }

    /// <summary>批量解锁队列</summary>
    /// <param name="mqs">待解锁的队列集合</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task UnlockBatchMQAsync(IList<MessageQueue> mqs, CancellationToken cancellationToken = default)
    {
        if (mqs == null || mqs.Count == 0) return;

        var groups = mqs.GroupBy(e => e.BrokerName);

        foreach (var g in groups)
        {
            using var span = Tracer?.NewSpan($"mq:{Name}:UnlockBatchMQ", g.Key);
            try
            {
                var bk = GetBroker(g.Key);
                if (bk == null) continue;

                var body = new
                {
                    consumerGroup = Group,
                    clientId = ClientId,
                    mqSet = g.Select(e => new { topic = e.Topic, brokerName = e.BrokerName, queueId = e.QueueId }).ToArray(),
                };

                await bk.InvokeAsync(RequestCode.UNLOCK_BATCH_MQ, body, null, false, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                span?.SetError(ex, null);
                WriteLog("解锁队列失败[{0}]：{1}", g.Key, ex.Message);
            }
        }
    }
    #endregion

    #region 消费调度

    private Task[] _tasks;
    private volatile Int32 _version;
    private CancellationTokenSource _source;

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
        //Stop();
        StopSchedule();

        // 如果有多个消费者，则等一段时间让大家停止消费，尽量避免重复消费
        //if (_Consumers != null && _Consumers.Length > 1) Thread.Sleep(10_000);

        // 释放资源
        if (_source != null)
        {
            _source.Cancel();
            _source.TryDispose();
        }

        var source = new CancellationTokenSource();
        WriteLog("正在创建[{0}]个消费线程，Group={1}，Topics={2}", qs.Length, Group, GetEffectiveTopics().Join(","));

        // 初始化并发限流信号量
        if (MaxConcurrentConsume > 0)
        {
            _concurrentSemaphore = new SemaphoreSlim(MaxConcurrentConsume, MaxConcurrentConsume);
            WriteLog("消费限流已启用，最大并发数={0}", MaxConcurrentConsume);
        }
        else
        {
            _concurrentSemaphore = null;
        }

        // 开线程
        var tasks = new Task[qs.Length];
        for (var i = 0; i < qs.Length; i++)
        {
            var queueStore = qs[i];
            tasks[i] = Task.Factory.StartNew(async () =>
            {
                await DoPull(queueStore, source.Token).ConfigureAwait(false);
            }, TaskCreationOptions.LongRunning);
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
        WriteLog("开始消费[{0}]，Group={1}，{2}，Offset={3}，CommitOffset={4}", mq.Topic ?? Topic, Group, mq, st.Offset, st.CommitOffset);

        // 顺序消费时先锁定队列
        if (OrderConsume)
        {
            var locked = await LockBatchMQAsync([mq], cancellationToken).ConfigureAwait(false);
            if (locked.Count == 0)
            {
                WriteLog("顺序消费锁定队列失败[{0}]，跳过消费", mq);
                return;
            }
        }

        var currentVersion = _version;
        while (currentVersion == _version && !cancellationToken.IsCancellationRequested)
        {
            DefaultSpan.Current = null;
            try
            {
                var offset = st.Offset;
                var pr = await Pull(mq, offset, BatchSize, SuspendTimeout, cancellationToken).ConfigureAwait(false);
                if (pr != null)
                {
                    switch (pr.Status)
                    {
                        case PullStatus.Found:
                            if (pr.Messages != null && pr.Messages.Length > 0)
                            {
                                DefaultSpan.Current = null;

                                // 限流：等待信号量
                                if (_concurrentSemaphore != null)
                                    await _concurrentSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                                // 性能埋点
                                using var span = Tracer?.NewSpan($"mq:{Name}:Consume", pr.Messages);
                                try
                                {
                                    // 触发消费
                                    var rs = await Consume(mq, pr, cancellationToken).ConfigureAwait(false);

                                    // 更新偏移
                                    if (rs)
                                    {
                                        st.Offset = pr.NextBeginOffset;
                                        // 提交消费进度
                                        await UpdateOffset(mq, st.Offset, cancellationToken).ConfigureAwait(false);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    span?.SetError(ex, mq);

                                    throw;
                                }
                                finally
                                {
                                    _concurrentSemaphore?.Release();
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
                            Log.Error("未知响应类型消息，序列[{1}]偏移量{0}", st.Offset, st.Queue.QueueId);
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
                await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            }
        }

        // 保存消费进度
        if (st.Offset >= 0 && st.Offset != st.CommitOffset)
        {
            var rs = await UpdateOffset(mq, st.Offset, cancellationToken).ConfigureAwait(false);
            st.CommitOffset = st.Offset;
        }

        // 顺序消费结束时解锁队列
        if (OrderConsume)
        {
            await UnlockBatchMQAsync([mq], cancellationToken).ConfigureAwait(false);
        }

        WriteLog("消费[{0}]结束", mq.Topic ?? Topic);
    }

    /// <summary>拉取到一批消息</summary>
    /// <param name="queue"></param>
    /// <param name="result"></param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    protected virtual async Task<Boolean> Consume(MessageQueue queue, PullResult result, CancellationToken cancellationToken)
    {
        if (Log != null && Log.Level <= LogLevel.Debug) WriteLog("{0}", result);

        var context = new ConsumeMessageContext
        {
            ConsumerGroup = Group,
            Mq = queue,
            MsgList = result.Messages.ToList(),
        };

        foreach (var hook in _consumeMessageHooks)
        {
            try
            {
                hook.ExecuteHookBefore(context);
            }
            catch (Exception e)
            {
                if (Log.Enable) Log.Error(e.Message);
            }
        }

        Consumed?.Invoke(this, new ConsumeEventArgs { Queue = queue, Messages = result.Messages, Result = result });

        var success = false;
        if (OnConsume != null) success = OnConsume(queue, result.Messages);
        if (OnConsumeAsync != null) success = await OnConsumeAsync(queue, result.Messages, cancellationToken).ConfigureAwait(false);

        // 消费失败且启用了重试时，将消息回退到RETRY Topic
        if (!success && EnableRetry && result.Messages != null)
        {
            foreach (var msg in result.Messages)
            {
                if (msg.ReconsumeTimes < MaxReconsumeTimes)
                {
                    try
                    {
                        await SendMessageBackAsync(msg, RetryDelayLevel, MaxReconsumeTimes, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        WriteLog("消息回退失败[{0}]：{1}", msg.MsgId, ex.Message);
                    }
                }
                else
                {
                    // 超过最大重试次数，消息将进入死信队列 %DLQ%{ConsumerGroup}
                    WriteLog("消息[{0}]超过最大重试次数[{1}]，将进入死信队列", msg.MsgId, MaxReconsumeTimes);
                }
            }
        }

        context.Success = success;

        foreach (var hook in _consumeMessageHooks)
        {
            try
            {
                hook.ExecuteHookAfter(context);
            }
            catch (Exception e)
            {
                if (Log.Enable) Log.Error(e.Message);
            }
        }

        return success;
    }

    private async Task PersistAll(IEnumerable<QueueStore> stores)
    {
        if (stores == null) return;

        // 广播模式保存到本地文件
        if (MessageModel == MessageModels.Broadcasting)
        {
            SaveLocalOffsets(stores.ToArray());
            return;
        }

        var ts = new List<Task>();
        using var source = new CancellationTokenSource(5_000);

        foreach (var item in stores)
        {
            if (item.Offset >= 0 && item.Offset != item.CommitOffset)
            {
                var mq = item.Queue;
                WriteLog("队列[{0}@{1}]更新偏移[{2:n0}]", mq.BrokerName, mq.QueueId, item.Offset);

                ts.Add(Task.Run(() => UpdateOffset(item.Queue, item.Offset, source.Token)));

                item.CommitOffset = item.Offset;
            }
        }

        await Task.WhenAll(ts).ConfigureAwait(false);
    }
    #endregion

    #region 消费端负载均衡

    /// <summary>当前所需要消费的队列。由均衡算法产生</summary>
    public MessageQueue[] Queues => _Queues?.Select(e => e.Queue).ToArray();

    private QueueStore[] _Queues;
    //private String[] _Consumers;

    class QueueStore
    {
        [XmlIgnore]
        public MessageQueue Queue { get; set; }
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

        public override String ToString() => Queue?.ToString();
        #endregion
    }

    /// <summary>重新平衡消费队列</summary>
    /// <returns></returns>
    public async Task<Boolean> Rebalance()
    {
        /*
         * 1，获取消费组下所有消费组，排序
         * 2，获取主题下所有队列，排序
         * 3，各消费者平均分配队列，不采用环形，减少消费者到Broker连接数
         */

        if (_Queues == null) WriteLog("准备从所有Broker服务器上获取消费者列表，以确定当前消费者应该负责消费的queue分片");

        var cs = await GetConsumers(Group).ConfigureAwait(false);
        if (cs.Count == 0) return false;

        // 为所有订阅主题构建队列列表，按Topic分别分配
        var allTopics = GetEffectiveTopics();
        var qs = new List<MessageQueue>();
        foreach (var topic in allTopics)
        {
            // 获取该Topic对应的Broker信息
            var topicBrokers = _NameServer?.GetTopicBrokers(topic) ?? Brokers;
            if (topicBrokers == null) continue;

            foreach (var br in topicBrokers)
            {
                if (br.Permission.HasFlag(Permissions.Read))
                {
                    for (var i = 0; i < br.ReadQueueNums; i++)
                    {
                        qs.Add(new MessageQueue { Topic = topic, BrokerName = br.Name, QueueId = i, });
                    }
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

            await PersistAll(ori).ConfigureAwait(false);
        }

        var dic = qs.GroupBy(e => e.BrokerName).ToDictionary(e => e.Key, e => e.Join(",", x => x.QueueId));
        var str = dic.Join(";", e => $"{e.Key}[{e.Value}]");
        WriteLog("消费重新平衡，当前消费者负责queue分片：{0}", str);

        using var span = Tracer?.NewSpan($"mq:{Name}:Rebalance", str);

        _Queues = rs.ToArray();
        await InitOffsetAsync().ConfigureAwait(false);
        //_Consumers = cs2.ToArray();

        return true;
    }

    private TimerX _timer;
    private DateTime _nextCheck;
    private Boolean _checking;

    /// <summary>检查消费组，如果消费者有变化，则需要重新平衡，重新分配各消费者所处理的队列</summary>
    /// <param name="state"></param>
    private async Task CheckGroup(Object state = null)
    {
        if (_checking) return;

        // 避免多次平衡同时进行
        var now = TimerX.Now;
        if (now < _nextCheck) return;

        //lock (this)
        //{
        if (_checking) return;
        _checking = true;

        using var span = Tracer?.NewSpan($"mq:{Name}:CheckGroup");
        try
        {
            var rs = await Rebalance().ConfigureAwait(false);
            if (!rs) return;

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
        //}
    }

    private async Task InitOffsetAsync(CancellationToken cancellationToken = default)
    {
        var qs = _Queues;
        if (qs == null || qs.Length == 0) return;

        // 广播模式优先从本地加载偏移
        if (MessageModel == MessageModels.Broadcasting)
        {
            var localOffsets = LoadLocalOffsets();
            foreach (var store in qs)
            {
                if (store.Offset >= 0) continue;

                var key = $"{store.Queue.Topic ?? Topic}@{store.Queue.BrokerName}@{store.Queue.QueueId}";
                // 兼容旧格式（不含Topic前缀）
                if (!localOffsets.TryGetValue(key, out var offset) || offset < 0)
                {
                    var oldKey = $"{store.Queue.BrokerName}@{store.Queue.QueueId}";
                    localOffsets.TryGetValue(oldKey, out offset);
                }
                if (offset >= 0)
                {
                    store.Offset = store.CommitOffset = offset;
                    WriteLog("从本地加载offset[{0}@{1}] Offset={2:n0}", store.Queue.BrokerName, store.Queue.QueueId, store.Offset);
                }
                else
                {
                    // 本地没有，从服务端查询
                    var off = FromLastOffset
                        ? await QueryMaxOffset(store.Queue, cancellationToken).ConfigureAwait(false)
                        : await QueryMinOffset(store.Queue, cancellationToken).ConfigureAwait(false);

                    store.Offset = store.CommitOffset = off;
                    WriteLog("初始化offset[{0}@{1}] Offset={2:n0}（广播模式）", store.Queue.BrokerName, store.Queue.QueueId, store.Offset);
                }
            }

            return;
        }

        var offsetTables = new Dictionary<MessageQueueModel, OffsetWrapperModel>();
        // 获取当前消费者分配到的服务器及服务器队列，按Topic+Broker分组查询
        var topicBrokerGroups = qs.Select(t => new { t.Queue.Topic, t.Queue.BrokerName })
            .Distinct()
            .GroupBy(t => t.Topic);
        foreach (var topicGroup in topicBrokerGroups)
        {
            var queryTopic = topicGroup.Key ?? Topic;
            foreach (var tb in topicGroup)
            {
                var broker = GetBroker(tb.BrokerName);
                var command = await broker.InvokeAsync(RequestCode.GET_CONSUME_STATS, null, new
                {
                    consumerGroup = Group,
                    topic = queryTopic
                }, true, cancellationToken).ConfigureAwait(false);
                var consumerStates = ConsumerStatesSpecialJsonHandler(command.Payload);
                //foreach (var (key, value) in consumerStates.OffsetTable) offsetTables.Add(key, value);
                foreach (var item in consumerStates.OffsetTable)
                {
                    // 避免重复添加（同一个Broker上不同Topic的key可能相同）
                    if (!offsetTables.ContainsKey(item.Key))
                        offsetTables.Add(item.Key, item.Value);
                }
            }
        }

        // 表示没消费过
        var neverConsumed = offsetTables.All(t => t.Value.ConsumerOffset == 0);

        foreach (var store in qs)
        {
            if (store.Offset >= 0) continue;

            // 先按BrokerName精确匹配，如果找不到（阿里云公网版BrokerName可能不一致），则按QueueId模糊匹配
            var item = offsetTables.FirstOrDefault(t => t.Key.BrokerName == store.Queue.BrokerName && t.Key.QueueId == store.Queue.QueueId);
            if (item.Value == null)
            {
                // 阿里云公网版RocketMQ，消费者状态返回的是真正brokerName，而前面Broker得到的是网关名
                item = offsetTables.FirstOrDefault(t => t.Key.QueueId == store.Queue.QueueId);
            }
            var offsetTable = item.Value ?? new OffsetWrapperModel();
            if (neverConsumed)
            {
                var offset = 0L;
                if (FromLastOffset)
                {
                    offset = offsetTable.BrokerOffset;
                    if (offset <= 0) offset = await QueryMaxOffset(store.Queue, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    offset = await QueryMinOffset(store.Queue, cancellationToken).ConfigureAwait(false);
                }

                store.Offset = store.CommitOffset = offset;
                await UpdateOffset(store.Queue, offset, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var offset = offsetTable.ConsumerOffset;
                if (offset <= 0) offset = await QueryOffset(store.Queue, cancellationToken).ConfigureAwait(false);

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
    private ConsumerStatesModel ConsumerStatesSpecialJsonHandler(IPacket payload)
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

        var consumerStatesModel = new ConsumerStatesModel() { OffsetTable = [] };

        for (var i = 0; i < offsetNew.Count / 2; i++)
        {
            var list = offsetNew.Skip(i * 2).Take(2).ToList();

            var messageQueue = list[0].ToJsonEntity<MessageQueueModel>();
            var offsetWrapper = list[1].ToJsonEntity<OffsetWrapperModel>();
            consumerStatesModel.OffsetTable.Add(messageQueue, offsetWrapper);
        }

        if (consumerStatesModel.OffsetTable.Count == 0)
        {
            WriteLog("无法解析消费者状态，可能是服务端版本不兼容，响应如下：");
            WriteLog(cmdStr);
        }

        return consumerStatesModel;
    }

    /// <summary>获取本地偏移存储路径</summary>
    private String GetOffsetFilePath()
    {
        var path = OffsetStorePath;
        if (String.IsNullOrEmpty(path))
            path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, ".offsets", $"{Group}.json");

        return path;
    }

    /// <summary>从本地文件加载偏移。广播模式使用</summary>
    private Dictionary<String, Int64> LoadLocalOffsets()
    {
        var file = GetOffsetFilePath();
        if (!File.Exists(file)) return [];

        try
        {
            var json = File.ReadAllText(file);
            if (String.IsNullOrWhiteSpace(json)) return [];

            return JsonHelper.Default.Read<Dictionary<String, Int64>>(json) ?? [];
        }
        catch
        {
            return [];
        }
    }

    /// <summary>保存偏移到本地文件。广播模式使用</summary>
    private void SaveLocalOffsets(QueueStore[] stores)
    {
        if (stores == null || stores.Length == 0) return;

        var dic = new Dictionary<String, Int64>();
        foreach (var st in stores)
        {
            if (st.Offset >= 0)
            {
                var key = $"{st.Queue.Topic ?? Topic}@{st.Queue.BrokerName}@{st.Queue.QueueId}";
                dic[key] = st.Offset;
            }
        }

        var file = GetOffsetFilePath();
        var dir = Path.GetDirectoryName(file);
        if (!String.IsNullOrEmpty(dir) && !Directory.Exists(dir))
            Directory.CreateDirectory(dir);

        File.WriteAllText(file, JsonHelper.Default.Write(dic));
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
        dic["PROP_CONSUMEORDERLY"] = OrderConsume ? "true" : "false";
        dic["PROP_CONSUMER_START_TIMESTAMP"] = StartTime.ToInt() + "";
        dic["PROP_CONSUME_TYPE"] = "CONSUME_PASSIVELY";
        dic["PROP_NAMESERVER_ADDR"] = NameServerAddress;
        dic["PROP_THREADPOOL_CORE_SIZE"] = (_tasks?.Length ?? 1).ToString();
        dic["messageModel"] = "CLUSTERING";
        ci.Properties = dic;

        var sd = new SubscriptionData { Topic = Topic, };
        ci.SubscriptionSet = GetEffectiveTopics().Select(t => new SubscriptionData { Topic = t }).ToArray();

        var sb = new StringBuilder();
        sb.Append('{');
        if (_Queues != null)
        {
            sb.Append("\"mqTable\":{");
            for (var i = 0; i < _Queues.Length; i++)
            {
                if (i > 0) sb.Append(',');

                var item = _Queues[i];

                sb.Append(JsonHost.Write(item.Queue, false, false, true));
                sb.Append(':');
                sb.Append(JsonHost.Write(item, false, false, true));
            }

            sb.Append('}');
        }
        {
            sb.Append(',');
            sb.Append("\"properties\":");
            sb.Append(JsonHost.Write(ci.Properties));
        }
        {
            sb.Append(',');
            sb.Append("\"subscriptionSet\":");
            sb.Append(JsonHost.Write(ci.SubscriptionSet, false, false, true));
        }
        sb.Append('}');

        var rs = cmd.CreateReply() as Command;
        rs.Header.Language = "DOTNET";
        rs.Payload = (ArrayPacket)sb.ToString().GetBytes();

        return rs;
    }

    #endregion

    #region 消费重试
    /// <summary>将消费失败的消息发回Broker，进入RETRY Topic延迟后重新消费</summary>
    /// <param name="msg">消费失败的消息</param>
    /// <param name="delayLevel">延迟等级。0=由Broker决定，大于0指定等级（1~18）</param>
    /// <param name="maxReconsumeTimes">最大重试次数。-1表示使用默认值</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>是否成功</returns>
    public async Task<Boolean> SendMessageBackAsync(MessageExt msg, Int32 delayLevel = 0, Int32 maxReconsumeTimes = -1, CancellationToken cancellationToken = default)
    {
        if (msg == null) throw new ArgumentNullException(nameof(msg));

        using var span = Tracer?.NewSpan($"mq:{Name}:SendMessageBack", msg.MsgId);
        try
        {
            var bk = GetBroker(msg.StoreHost?.Split(':').FirstOrDefault() ?? Brokers?.FirstOrDefault()?.Name);
            if (bk == null)
            {
                // 尝试在所有Broker上发送
                bk = Clients?.FirstOrDefault();
            }
            if (bk == null) return false;

            var header = new
            {
                group = Group,
                offset = msg.CommitLogOffset,
                delayLevel = delayLevel > 0 ? delayLevel : RetryDelayLevel,
                originMsgId = msg.MsgId ?? "",
                originTopic = msg.Topic ?? Topic,
                unitMode = UnitMode,
                maxReconsumeTimes = maxReconsumeTimes >= 0 ? maxReconsumeTimes : MaxReconsumeTimes,
            };

            await bk.InvokeAsync(RequestCode.CONSUMER_SEND_MSG_BACK, null, header, true, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            WriteLog("消息回退失败：{0}", ex.Message);
            return false;
        }
    }

    /// <summary>将消费失败的消息发回Broker，进入RETRY Topic延迟后重新消费</summary>
    /// <param name="msg">消费失败的消息</param>
    /// <param name="delayLevel">延迟等级</param>
    /// <param name="maxReconsumeTimes">最大重试次数</param>
    /// <returns>是否成功</returns>
    public Boolean SendMessageBack(MessageExt msg, Int32 delayLevel = 0, Int32 maxReconsumeTimes = -1)
        => SendMessageBackAsync(msg, delayLevel, maxReconsumeTimes).ConfigureAwait(false).GetAwaiter().GetResult();
    #endregion

    #region Pop消费模式
    /// <summary>Pop方式拉取消息。5.0新增的轻量消费模式，无需客户端Rebalance</summary>
    /// <param name="brokerName">Broker名称</param>
    /// <param name="maxNums">最大拉取数</param>
    /// <param name="invisibleTime">不可见时间（毫秒），消息被拉取后在此时间内不会被其他消费者看到</param>
    /// <param name="pollTime">长轮询等待时间（毫秒）</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>拉取结果</returns>
    public async Task<PullResult> PopMessageAsync(String brokerName, Int32 maxNums = 32, Int64 invisibleTime = 60_000, Int32 pollTime = 15_000, CancellationToken cancellationToken = default)
    {
        if (String.IsNullOrEmpty(brokerName)) throw new ArgumentNullException(nameof(brokerName));

        using var span = Tracer?.NewSpan($"mq:{Name}:PopMessage", brokerName);
        try
        {
            var bk = GetBroker(brokerName);
            if (bk == null) return null;

            var header = new
            {
                consumerGroup = Group,
                topic = Topic,
                maxMsgNums = maxNums,
                invisibleTime,
                pollTime,
                bornTime = DateTime.Now.ToLong(),
                initMode = FromLastOffset ? 1 : 0,
                expType = ExpressionType,
                exp = Subscription,
            };

            var rs = await bk.InvokeAsync(RequestCode.POP_MESSAGE, null, header, true, cancellationToken).ConfigureAwait(false);
            if (rs?.Header == null) return null;

            var pr = new PullResult();
            if (rs.Header.Code == 0)
                pr.Status = PullStatus.Found;
            else if (rs.Header.Code == (Int32)ResponseCode.PULL_NOT_FOUND)
                pr.Status = PullStatus.NoNewMessage;
            else
                pr.Status = PullStatus.Unknown;

            pr.Read(rs.Header?.ExtFields);

            if (rs.Payload != null) pr.Messages = MessageExt.ReadAll(rs.Payload).ToArray();

            return pr;
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }
    }

    /// <summary>确认Pop消息消费完成</summary>
    /// <param name="brokerName">Broker名称</param>
    /// <param name="extraInfo">消息额外信息，Pop拉取时返回</param>
    /// <param name="offset">消息偏移</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<Boolean> AckMessageAsync(String brokerName, String extraInfo, Int64 offset, CancellationToken cancellationToken = default)
    {
        using var span = Tracer?.NewSpan($"mq:{Name}:AckMessage", offset);
        try
        {
            var bk = GetBroker(brokerName);
            if (bk == null) return false;

            var header = new
            {
                consumerGroup = Group,
                topic = Topic,
                extraInfo,
                offset,
            };

            await bk.InvokeAsync(RequestCode.ACK_MESSAGE, null, header, true, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            WriteLog("确认Pop消息失败：{0}", ex.Message);
            return false;
        }
    }

    /// <summary>修改Pop消息不可见时间，延长消费处理窗口</summary>
    /// <param name="brokerName">Broker名称</param>
    /// <param name="extraInfo">消息额外信息</param>
    /// <param name="offset">消息偏移</param>
    /// <param name="invisibleTime">新的不可见时间（毫秒）</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<Boolean> ChangeInvisibleTimeAsync(String brokerName, String extraInfo, Int64 offset, Int64 invisibleTime, CancellationToken cancellationToken = default)
    {
        using var span = Tracer?.NewSpan($"mq:{Name}:ChangeInvisibleTime", offset);
        try
        {
            var bk = GetBroker(brokerName);
            if (bk == null) return false;

            var header = new
            {
                consumerGroup = Group,
                topic = Topic,
                extraInfo,
                offset,
                invisibleTime,
            };

            await bk.InvokeAsync(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, null, header, true, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            WriteLog("修改不可见时间失败：{0}", ex.Message);
            return false;
        }
    }
    #endregion

#if NETSTANDARD2_1_OR_GREATER
    #region gRPC消费模式
    /// <summary>通过gRPC协议接收消息。需要先设置GrpcProxyAddress</summary>
    /// <param name="queue">gRPC消息队列</param>
    /// <param name="filterExpression">过滤表达式。默认Tag=*</param>
    /// <param name="batchSize">批量大小。默认32</param>
    /// <param name="invisibleDuration">不可见时间。默认30秒</param>
    /// <param name="longPollingTimeout">长轮询超时。默认20秒</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>gRPC消息列表</returns>
    public async Task<IList<Grpc.GrpcMessage>> ReceiveMessageViaGrpcAsync(
        Grpc.GrpcMessageQueue queue,
        Grpc.GrpcFilterExpression filterExpression = null,
        Int32 batchSize = 32,
        TimeSpan? invisibleDuration = null,
        TimeSpan? longPollingTimeout = null,
        CancellationToken cancellationToken = default)
    {
        if (_GrpcService == null) throw new InvalidOperationException("gRPC service not initialized. Set GrpcProxyAddress first.");

        using var span = Tracer?.NewSpan($"mq:{Name}:ReceiveMessage:grpc");
        try
        {
            return await _GrpcService.ReceiveMessageAsync(
                Group,
                queue,
                filterExpression,
                batchSize,
                invisibleDuration,
                longPollingTimeout,
                cancellationToken
            ).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }
    }

    /// <summary>通过gRPC协议确认消息</summary>
    /// <param name="topic">主题</param>
    /// <param name="entries">确认条目列表</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>确认结果</returns>
    public async Task<Grpc.AckMessageResponse> AckMessageViaGrpcAsync(
        String topic,
        IList<Grpc.AckMessageEntry> entries,
        CancellationToken cancellationToken = default)
    {
        if (_GrpcService == null) throw new InvalidOperationException("gRPC service not initialized. Set GrpcProxyAddress first.");

        using var span = Tracer?.NewSpan($"mq:{Name}:AckMessage:grpc");
        try
        {
            return await _GrpcService.AckMessageAsync(topic, Group, entries, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }
    }

    /// <summary>通过gRPC协议查询队列分配</summary>
    /// <param name="topic">主题。默认使用当前Topic</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>分配结果</returns>
    public async Task<Grpc.QueryAssignmentResponse> QueryAssignmentViaGrpcAsync(String topic = null, CancellationToken cancellationToken = default)
    {
        if (_GrpcService == null) throw new InvalidOperationException("gRPC service not initialized. Set GrpcProxyAddress first.");

        return await _GrpcService.QueryAssignmentAsync(topic ?? Topic, Group, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>通过gRPC协议修改消息不可见时间</summary>
    /// <param name="topic">主题</param>
    /// <param name="receiptHandle">收据句柄</param>
    /// <param name="messageId">消息ID</param>
    /// <param name="invisibleDuration">新的不可见时间</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<Grpc.ChangeInvisibleDurationResponse> ChangeInvisibleDurationViaGrpcAsync(
        String topic,
        String receiptHandle,
        String messageId,
        TimeSpan invisibleDuration,
        CancellationToken cancellationToken = default)
    {
        if (_GrpcService == null) throw new InvalidOperationException("gRPC service not initialized. Set GrpcProxyAddress first.");

        return await _GrpcService.ChangeInvisibleDurationAsync(topic, Group, receiptHandle, messageId, invisibleDuration, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>通过gRPC协议发送心跳</summary>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<Grpc.HeartbeatResponse> HeartbeatViaGrpcAsync(CancellationToken cancellationToken = default)
    {
        if (_GrpcService == null) throw new InvalidOperationException("gRPC service not initialized. Set GrpcProxyAddress first.");

        return await _GrpcService.HeartbeatAsync(Group, Grpc.GrpcClientType.SIMPLE_CONSUMER, cancellationToken).ConfigureAwait(false);
    }
    #endregion
#endif

    #region Request-Reply 请求响应模式
    /// <summary>发送回复消息</summary>
    /// <param name="requestMessage">原始请求消息</param>
    /// <param name="replyBody">回复消息体</param>
    /// <returns>发送结果</returns>
    public virtual SendResult SendReply(MessageExt requestMessage, Object replyBody)
    {
        if (requestMessage == null) throw new ArgumentNullException(nameof(requestMessage));
        if (replyBody == null) throw new ArgumentNullException(nameof(replyBody));

        // 检查是否有回复地址
        var replyToClient = requestMessage.ReplyToClient;
        if (String.IsNullOrEmpty(replyToClient))
        {
            throw new InvalidOperationException("Request message does not have ReplyToClient property");
        }

        var correlationId = requestMessage.CorrelationId;
        if (String.IsNullOrEmpty(correlationId))
        {
            throw new InvalidOperationException("Request message does not have CorrelationId property");
        }

        // 创建回复消息
        var replyMessage = new Message
        {
            Topic = requestMessage.Topic,
            CorrelationId = correlationId,
            MessageType = "REPLY"
        };
        replyMessage.SetBody(replyBody);

        // 使用临时Producer发送回复
        using var producer = new Producer
        {
            NameServerAddress = NameServerAddress,
            Topic = requestMessage.Topic,
            Group = Group + "_REPLY",
            ClientIP = ClientIP,
            InstanceName = InstanceName,
            Log = Log,
            Tracer = Tracer
        };
        producer.Start();

        return producer.Publish(replyMessage);
    }

    /// <summary>发送回复消息(异步)</summary>
    /// <param name="requestMessage">原始请求消息</param>
    /// <param name="replyBody">回复消息体</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>发送结果</returns>
    public virtual async Task<SendResult> SendReplyAsync(MessageExt requestMessage, Object replyBody, CancellationToken cancellationToken = default)
    {
        if (requestMessage == null) throw new ArgumentNullException(nameof(requestMessage));
        if (replyBody == null) throw new ArgumentNullException(nameof(replyBody));

        // 检查是否有回复地址
        var replyToClient = requestMessage.ReplyToClient;
        if (String.IsNullOrEmpty(replyToClient))
        {
            throw new InvalidOperationException("Request message does not have ReplyToClient property");
        }

        var correlationId = requestMessage.CorrelationId;
        if (String.IsNullOrEmpty(correlationId))
        {
            throw new InvalidOperationException("Request message does not have CorrelationId property");
        }

        // 创建回复消息
        var replyMessage = new Message
        {
            Topic = requestMessage.Topic,
            CorrelationId = correlationId,
            MessageType = "REPLY"
        };
        replyMessage.SetBody(replyBody);

        // 使用临时Producer发送回复
        using var producer = new Producer
        {
            NameServerAddress = NameServerAddress,
            Topic = requestMessage.Topic,
            Group = Group + "_REPLY",
            ClientIP = ClientIP,
            InstanceName = InstanceName,
            Log = Log,
            Tracer = Tracer
        };
        producer.Start();

        return await producer.PublishAsync(replyMessage, null, cancellationToken).ConfigureAwait(false);
    }

    #endregion
}