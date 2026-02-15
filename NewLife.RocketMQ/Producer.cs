using System.Collections.Concurrent;
using System.Globalization;
using NewLife.Log;
using NewLife.Reflection;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Common;
using NewLife.RocketMQ.MessageTrace;
using NewLife.RocketMQ.Models;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ;

/// <summary>生产者</summary>
public class Producer : MqBase
{
    private const Int32 CommitLogOffsetHexLength = 16;

    #region 属性
    /// <summary>负载均衡。发布消息时，分发到各个队列的负载均衡算法，默认使用带权重的轮询</summary>
    public ILoadBalance LoadBalance { get; set; }

    //public Int32 DefaultTopicQueueNums { get; set; } = 4;

    //public Int32 SendMsgTimeout { get; set; } = 3_000;

    //public Int32 CompressMsgBodyOverHowmuch { get; set; } = 4096;

    /// <summary>发送消息失败时的重试次数。默认3次</summary>
    public Int32 RetryTimesWhenSendFailed { get; set; } = 3;

    //public Int32 RetryTimesWhenSendAsyncFailed { get; set; } = 2;

    //public Boolean RetryAnotherBrokerWhenNotStoreOK { get; set; }

    /// <summary>最大消息大小。默认4*1024*1024</summary>
    public Int32 MaxMessageSize { get; set; } = 4 * 1024 * 1024;

    /// <summary>消息体压缩阈值（字节）。超过该大小自动ZLIB压缩，默认4096。设为0则禁用压缩</summary>
    public Int32 CompressOverBytes { get; set; } = 4096;

    private readonly IList<ISendMessageHook> _sendMessageHooks = new List<ISendMessageHook>();
    private AsyncTraceDispatcher _traceDispatcher;

    /// <summary>请求超时时间。默认3000ms</summary>
    public Int32 RequestTimeout { get; set; } = 3_000;

    /// <summary>事务回查委托。Broker发起事务回查时调用，参数为消息和事务ID，返回事务状态</summary>
    public Func<MessageExt, String, TransactionState> OnCheckTransaction;

    /// <summary>异步事务回查委托。Broker发起事务回查时调用，参数为消息、事务ID和取消令牌，返回事务状态</summary>
    public Func<MessageExt, String, CancellationToken, Task<TransactionState>> OnCheckTransactionAsync;

    private readonly ConcurrentDictionary<String, TaskCompletionSource<MessageExt>> _requestCallbacks = new();
    private Consumer _replyConsumer;
    private String _replyTopic;
    #endregion

    #region 基础方法
    /// <summary>启动</summary>
    /// <returns></returns>
    protected override void OnStart()
    {
        base.OnStart();

        if (EnableMessageTrace)
        {
            _traceDispatcher = new AsyncTraceDispatcher();
            _traceDispatcher.Start(NameServerAddress);
            _sendMessageHooks.Add(new MessageTraceHook(_traceDispatcher));
        }

        LoadBalance ??= new WeightRoundRobin();

        if (_NameServer != null)
        {
            _NameServer.OnBrokerChange += (s, e) =>
            {
                _brokers = null;
                //_robin = null;
                LoadBalance.Ready = false;
            };
        }

        // 初始化回复消息消费者
        _replyTopic = $"{Topic}_REPLY_{ClientId}";
    }

    /// <summary>停止</summary>
    protected override void OnStop()
    {
        // 停止回复消息消费者
        if (_replyConsumer != null)
        {
            _replyConsumer.Stop();
            _replyConsumer.Dispose();
            _replyConsumer = null;
        }

        base.OnStop();
    }
    #endregion

    #region 发布消息（普通/顺序）
    /// <summary>发送消息</summary>
    /// <param name="message">消息体</param>
    /// <param name="queue">目标队列。指定时可实现顺序发布（通过SelectQueue获取），默认未指定并自动选择队列</param>
    /// <param name="timeout"></param>
    /// <returns></returns>
    public virtual SendResult Publish(Message message, MessageQueue queue, Int32 timeout = -1)
    {
        // 构造请求头
        var header = CreateHeader(message);

        for (var i = 0; i <= RetryTimesWhenSendFailed; i++)
        {
            // 选择队列分片
            var mq = queue ?? SelectQueue();
            mq.Topic = Topic;
            header.QueueId = mq.QueueId;
            header.BrokerName = mq.BrokerName;

            // 性能埋点
            using var span = Tracer?.NewSpan($"mq:{Name}:Publish", message.BodyString);
            span?.AppendTag($"queue={mq}");

            SendMessageContext context = null;
            try
            {
                // 根据队列获取Broker客户端
                var bk = GetBroker(mq.BrokerName);

                context = new SendMessageContext
                {
                    ProducerGroup = Group,
                    Message = message,
                    Mq = mq,
                    BrokerAddr = bk.Name,
                };
                foreach (var hook in _sendMessageHooks)
                {
                    try { hook.ExecuteHookBefore(context); }
                    catch (Exception e) 
                    { 
                        if (Log.Enable) Log.Error(e.Message); 
                    }
                }

                var rs = bk.Invoke(RequestCode.SEND_MESSAGE_V2, message.Body, header.GetProperties(), true);

                // 包装结果
                var result = new SendResult
                {
                    Queue = mq,
                    Header = rs.Header,
                    Status = (ResponseCode)rs.Header.Code switch
                    {
                        ResponseCode.SUCCESS => SendStatus.SendOK,
                        ResponseCode.FLUSH_DISK_TIMEOUT => SendStatus.FlushDiskTimeout,
                        ResponseCode.FLUSH_SLAVE_TIMEOUT => SendStatus.FlushSlaveTimeout,
                        ResponseCode.SLAVE_NOT_AVAILABLE => SendStatus.SlaveNotAvailable,
                        _ => throw rs.Header.CreateException(),
                    }
                };
                result.Read(rs.Header?.ExtFields);

                context.SendResult = result;
                foreach (var hook in _sendMessageHooks)
                {
                    try { hook.ExecuteHookAfter(context); }
                    catch (Exception e) { if (Log.Enable) Log.Error(e.Message); }
                }

                span?.AppendTag($"Status={result.Status}");
                span?.AppendTag($"MsgId={result.MsgId}");
                span?.AppendTag($"OffsetMsgId={result.OffsetMsgId}");
                span?.AppendTag($"QueueOffset={result.QueueOffset}");
                span?.AppendTag($"TransactionId={result.TransactionId}");

                if (Log != null && Log.Level <= LogLevel.Debug) WriteLog("{0}", result);

                return result;
            }
            catch (Exception ex)
            {
                if (context != null)
                {
                    context.E = ex;
                    foreach (var hook in _sendMessageHooks)
                    {
                        try { hook.ExecuteHookAfter(context); }
                        catch (Exception e) { if (Log.Enable) Log.Error(e.Message); }
                    }
                }

                // 如果网络异常，则延迟重发
                if (i < RetryTimesWhenSendFailed)
                {
                    Thread.Sleep(1000);
                    continue;
                }

                span?.SetError(ex, message);

                throw;
            }
        }

        return null;
    }

    /// <summary>发布消息</summary>
    /// <param name="body"></param>
    /// <param name="timeout"></param>
    /// <returns></returns>
    public virtual SendResult Publish(Object body, Int32 timeout = -1) => Publish(CreateMessage(body), null, timeout);

    /// <summary>发布消息</summary>
    /// <param name="body"></param>
    /// <param name="tags"></param>
    /// <param name="timeout"></param>
    /// <returns></returns>
    public virtual SendResult Publish(Object body, String tags, Int32 timeout = -1)
    {
        var message = CreateMessage(body);
        message.Tags = tags;

        return Publish(message, null, timeout);
    }

    /// <summary>发布消息</summary>
    /// <param name="body"></param>
    /// <param name="tags">传null则为空</param>
    /// <param name="keys">传null则为空</param>
    /// <param name="timeout"></param>
    /// <returns></returns>
    public virtual SendResult Publish(Object body, String tags, String keys, Int32 timeout = -1)
    {
        var message = CreateMessage(body);
        message.Tags = tags;
        message.Keys = keys;

        return Publish(message, null, timeout);
    }

    /// <summary>发布事务消息（半消息）</summary>
    /// <param name="message">消息体</param>
    /// <param name="queue">目标队列</param>
    /// <param name="timeout"></param>
    /// <returns></returns>
    public virtual SendResult PublishTransaction(Message message, MessageQueue queue = null, Int32 timeout = -1)
    {
        if (message is null) throw new ArgumentNullException(nameof(message));

        message.Properties["TRAN_MSG"] = "true";
        message.Properties["PGROUP"] = Group;

        return Publish(message, queue, timeout);
    }

    /// <summary>发布事务消息（半消息）</summary>
    /// <param name="body"></param>
    /// <param name="tags"></param>
    /// <param name="keys"></param>
    /// <param name="timeout"></param>
    /// <returns></returns>
    public virtual SendResult PublishTransaction(Object body, String tags = null, String keys = null, Int32 timeout = -1)
    {
        var message = CreateMessage(body);
        message.Tags = tags;
        message.Keys = keys;

        return PublishTransaction(message, null, timeout);
    }
    #endregion

    #region 异步发布消息
    /// <summary>发布消息</summary>
    /// <param name="message">消息体</param>
    /// <param name="queue">目标队列。指定时可实现顺序发布（通过SelectQueue获取），默认未指定并自动选择队列</param>
    /// <param name="cancellationToken"></param>
    public virtual async Task<SendResult> PublishAsync(Message message, MessageQueue queue, CancellationToken cancellationToken = default)
    {
        if (message is null) throw new ArgumentNullException(nameof(message));

#if NETSTANDARD2_1_OR_GREATER
        // gRPC模式
        if (_GrpcService != null)
            return await PublishViaGrpcAsync(message, cancellationToken).ConfigureAwait(false);
#endif

        // 构造请求头
        var header = CreateHeader(message);

        for (var i = 0; i <= RetryTimesWhenSendFailed; i++)
        {
            // 选择队列分片
            var mq = queue ?? SelectQueue();
            mq.Topic = Topic;
            header.QueueId = mq.QueueId;

            // 性能埋点
            using var span = Tracer?.NewSpan($"mq:{Name}:PublishAsync", message.BodyString);
            try
            {
                var bk = GetBroker(mq.BrokerName);

                var context = new SendMessageContext();
                foreach (var hook in _sendMessageHooks)
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

                var rs = await bk.InvokeAsync(RequestCode.SEND_MESSAGE_V2, message.Body, header.GetProperties(), true, cancellationToken).ConfigureAwait(false);

                foreach (var hook in _sendMessageHooks)
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

                // 包装结果
                var sendResult = new SendResult
                {
                    Queue = mq,
                    Header = rs.Header,
                    Status = (ResponseCode)rs.Header.Code switch
                    {
                        ResponseCode.SUCCESS => SendStatus.SendOK,
                        ResponseCode.FLUSH_DISK_TIMEOUT => SendStatus.FlushDiskTimeout,
                        ResponseCode.FLUSH_SLAVE_TIMEOUT => SendStatus.FlushSlaveTimeout,
                        ResponseCode.SLAVE_NOT_AVAILABLE => SendStatus.SlaveNotAvailable,
                        _ => throw rs.Header.CreateException(),
                    }
                };
                sendResult.Read(rs.Header?.ExtFields);

                return sendResult;
            }
            catch (Exception ex)
            {
                // 如果网络异常，则延迟重发
                if (i < RetryTimesWhenSendFailed)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                    continue;
                }

                span?.SetError(ex, message);

                throw;
            }
        }

        return null;
    }

    /// <summary>异步发布事务消息（半消息）</summary>
    /// <param name="message">消息体</param>
    /// <param name="queue">目标队列</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public virtual Task<SendResult> PublishTransactionAsync(Message message, MessageQueue queue = null, CancellationToken cancellationToken = default)
    {
        if (message is null) throw new ArgumentNullException(nameof(message));

        message.Properties["TRAN_MSG"] = "true";
        message.Properties["PGROUP"] = Group;

        return PublishAsync(message, queue, cancellationToken);
    }

    /// <summary>发布消息</summary>
    /// <param name="body"></param>
    /// <returns></returns>
    public virtual Task<SendResult> PublishAsync(Object body) => PublishAsync(CreateMessage(body), null);

    /// <summary>发布消息</summary>
    /// <param name="body"></param>
    /// <param name="tags">传null则为空</param>
    /// <param name="keys">传null则为空</param>
    /// <returns></returns>
    public virtual Task<SendResult> PublishAsync(Object body, String tags, String keys)
    {
        var message = CreateMessage(body);
        message.Tags = tags;
        message.Keys = keys;

        return PublishAsync(message, null);
    }

#if NETSTANDARD2_1_OR_GREATER
    /// <summary>通过gRPC协议发送消息</summary>
    /// <param name="message">消息</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    private async Task<SendResult> PublishViaGrpcAsync(Message message, CancellationToken cancellationToken)
    {
        using var span = Tracer?.NewSpan($"mq:{Name}:PublishAsync:grpc", message.BodyString);
        try
        {
            var keys = message.Keys?.Split(',').Where(k => !String.IsNullOrEmpty(k)).ToList();
            var rs = await _GrpcService.SendMessageAsync(
                Topic,
                message.Body,
                tag: message.Tags,
                keys: keys,
                properties: message.Properties.Count > 0 ? message.Properties : null,
                cancellationToken: cancellationToken
            ).ConfigureAwait(false);

            if (rs.Status?.Code != Grpc.GrpcCode.OK)
                throw new InvalidOperationException($"gRPC SendMessage failed: {rs.Status}");

            var entry = rs.Entries.FirstOrDefault();
            var result = new SendResult
            {
                Status = SendStatus.SendOK,
                MsgId = entry?.MessageId,
                TransactionId = entry?.TransactionId,
                QueueOffset = entry?.Offset ?? 0,
            };

            return result;
        }
        catch (Exception ex)
        {
            span?.SetError(ex, message);
            throw;
        }
    }
#endif
    #endregion

    #region 发布单向消息
    /// <summary>发送消息，不等结果</summary>
    /// <param name="message">消息体</param>
    /// <param name="queue">目标队列。指定时可实现顺序发布（通过SelectQueue获取），默认未指定并自动选择队列</param>
    /// <returns></returns>
    public virtual SendResult PublishOneway(Message message, MessageQueue queue)
    {
        // 构造请求头
        var header = CreateHeader(message);

        for (var i = 0; i <= RetryTimesWhenSendFailed; i++)
        {
            // 选择队列分片
            var mq = queue ?? SelectQueue();
            mq.Topic = Topic;
            header.QueueId = mq.QueueId;

            // 性能埋点
            using var span = Tracer?.NewSpan($"mq:{Name}:PublishOneway", message.BodyString);
            try
            {
                // 根据队列获取Broker客户端
                var bk = GetBroker(mq.BrokerName);

                var context = new SendMessageContext
                {
                    ProducerGroup = Group,
                    Message = message,
                    Mq = mq,
                    BrokerAddr = bk.Name,
                };
                foreach (var hook in _sendMessageHooks)
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

                var rs = bk.InvokeOneway(RequestCode.SEND_MESSAGE_V2, message.Body, header.GetProperties());

                // 包装结果
                var sendResult = new SendResult
                {
                    Queue = mq,
                    Header = rs.Header,
                    Status = rs.Header.Code switch
                    {
                        -1 => SendStatus.SendError,

                        _ => SendStatus.SendOK,
                    }
                };

                context.SendResult = sendResult;
                foreach (var hook in _sendMessageHooks)
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

                if (Log != null && Log.Level <= LogLevel.Debug) WriteLog("{0}", sendResult);

                return sendResult;

            }
            catch (Exception ex)
            {
                // 如果网络异常，则延迟重发
                if (i < RetryTimesWhenSendFailed)
                {
                    Thread.Sleep(1000);
                    continue;
                }

                span?.SetError(ex, message);

                throw;
            }
        }
        return null;
    }

    /// <summary>发布消息，不等结果</summary>
    /// <param name="body"></param>
    /// <param name="tags"></param>
    /// <returns></returns>
    public virtual void PublishOneway(Object body, String tags = null)
    {
        var message = CreateMessage(body);
        message.Tags = tags;

        PublishOneway(message, null);
    }
    #endregion

    #region 批量发布消息
    /// <summary>批量发布消息</summary>
    /// <param name="messages">消息集合</param>
    /// <param name="timeout">超时时间</param>
    /// <returns></returns>
    public virtual SendResult PublishBatch(IList<Message> messages, Int32 timeout = -1)
    {
        if (messages == null || messages.Count == 0) throw new ArgumentException("消息集合不能为空", nameof(messages));

        // 编码批量消息体
        var ms = new MemoryStream();
        var bn = new NewLife.Serialization.Binary { Stream = ms, IsLittleEndian = false };
        foreach (var msg in messages)
        {
            msg.Topic ??= Topic;
            var body = msg.Body ?? new Byte[0];
            var props = msg.GetProperties() ?? "";
            var propsBytes = props.GetBytes();

            // 按照 RocketMQ 批量消息编码格式
            // TotalSize(4) + MagicCode(4) + BodyCRC(4) + Flag(4) + BodyLen(4) + Body + PropsLen(2) + Props
            var totalSize = 4 + 4 + 4 + 4 + 4 + body.Length + 2 + propsBytes.Length;
            bn.Write(totalSize);
            bn.Write(0); // MagicCode
            bn.Write(0); // BodyCRC
            bn.Write(msg.Flag);
            bn.Write(body.Length);
            ms.Write(body, 0, body.Length);
            bn.Write((Int16)propsBytes.Length);
            ms.Write(propsBytes, 0, propsBytes.Length);
        }

        var batchBody = ms.ToArray();

        // 使用第一条消息的属性作为批量消息头
        var firstMsg = messages[0];
        var header = new SendMessageRequestHeader
        {
            ProducerGroup = Group,
            Topic = Topic,
            SysFlag = 0,
            BornTimestamp = DateTime.UtcNow.ToLong(),
            Flag = firstMsg.Flag,
            Properties = firstMsg.GetProperties(),
            ReconsumeTimes = 0,
            UnitMode = UnitMode,
            Batch = true,
            DefaultTopic = DefaultTopic,
            DefaultTopicQueueNums = DefaultTopicQueueNums
        };

        // 选择队列分片
        var mq = SelectQueue();
        if (mq == null) return null;

        mq.Topic = Topic;
        header.QueueId = mq.QueueId;
        header.BrokerName = mq.BrokerName;

        using var span = Tracer?.NewSpan($"mq:{Name}:PublishBatch", messages.Count);
        try
        {
            var bk = GetBroker(mq.BrokerName);
            var rs = bk.Invoke(RequestCode.SEND_BATCH_MESSAGE, batchBody, header.GetProperties(), true);

            var result = new SendResult
            {
                Queue = mq,
                Header = rs.Header,
                Status = (ResponseCode)rs.Header.Code switch
                {
                    ResponseCode.SUCCESS => SendStatus.SendOK,
                    ResponseCode.FLUSH_DISK_TIMEOUT => SendStatus.FlushDiskTimeout,
                    ResponseCode.FLUSH_SLAVE_TIMEOUT => SendStatus.FlushSlaveTimeout,
                    ResponseCode.SLAVE_NOT_AVAILABLE => SendStatus.SlaveNotAvailable,
                    _ => throw rs.Header.CreateException(),
                }
            };
            result.Read(rs.Header?.ExtFields);

            return result;
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }
    }

    /// <summary>批量发布字符串消息</summary>
    /// <param name="bodies">消息体集合</param>
    /// <param name="tags">标签</param>
    /// <returns></returns>
    public virtual SendResult PublishBatch(IList<String> bodies, String tags = null)
    {
        var messages = new List<Message>();
        foreach (var body in bodies)
        {
            var msg = new Message();
            msg.SetBody(body);
            if (tags != null) msg.Tags = tags;
            messages.Add(msg);
        }

        return PublishBatch(messages);
    }
    #endregion

    #region 发布延迟消息
    /// <summary>发布延迟消息</summary>
    /// <param name="message">消息体</param>
    /// <param name="queue">目标队列。指定时可实现顺序发布（通过SelectQueue获取），默认未指定并自动选择队列</param>
    /// <param name="level">延迟时间等级。18级</param>
    /// <returns></returns>
    public virtual SendResult PublishDelay(Message message, MessageQueue queue, DelayTimeLevels level)
    {
        // 构造请求头
        message.DelayTimeLevel = (Int32)level;
        var header = CreateHeader(message);

        for (var i = 0; i <= RetryTimesWhenSendFailed; i++)
        {
            // 选择队列分片
            var mq = queue ?? SelectQueue();
            mq.Topic = Topic;
            header.QueueId = mq.QueueId;

            // 性能埋点
            using var span = Tracer?.NewSpan($"mq:{Name}:PublishDelay", new { level, message.BodyString });
            try
            {
                // 根据队列获取Broker客户端
                var bk = GetBroker(mq.BrokerName);

                var context = new SendMessageContext
                {
                    ProducerGroup = Group,
                    Message = message,
                    Mq = mq,
                    BrokerAddr = bk.Name,
                };
                foreach (var hook in _sendMessageHooks)
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

                var rs = bk.InvokeOneway(RequestCode.SEND_MESSAGE_V2, message.Body, header.GetProperties());

                // 包装结果
                var sendResult = new SendResult
                {
                    Queue = mq,
                    Header = rs.Header,
                    Status = rs.Header.Code switch
                    {
                        -1 => SendStatus.SendError,

                        _ => SendStatus.SendOK,
                    }
                };

                context.SendResult = sendResult;
                foreach (var hook in _sendMessageHooks)
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

                if (Log != null && Log.Level <= LogLevel.Debug) WriteLog("{0}", sendResult);

                return sendResult;

            }
            catch (Exception ex)
            {
                // 如果网络异常，则延迟重发
                if (i < RetryTimesWhenSendFailed)
                {
                    Thread.Sleep(1000);
                    continue;
                }

                span?.SetError(ex, message);

                throw;
            }
        }
        return null;
    }

    /// <summary>发布延迟消息</summary>
    /// <param name="body"></param>
    /// <param name="level">延迟时间等级。18级</param>
    /// <param name="tags"></param>
    /// <returns></returns>
    public virtual void PublishDelay(Object body, DelayTimeLevels level, String tags = null)
    {
        var message = CreateMessage(body);
        message.Tags = tags;

        PublishDelay(message, null, level);
    }
    #endregion

    #region 结束事务消息
    /// <summary>结束事务消息。提交或回滚事务</summary>
    /// <param name="result">发布事务消息返回结果</param>
    /// <param name="state">事务状态</param>
    /// <param name="fromTransactionCheck">是否来自事务回查</param>
    public virtual void EndTransaction(SendResult result, TransactionState state, Boolean fromTransactionCheck = false)
    {
        if (result is null) throw new ArgumentNullException(nameof(result));
        if (result.Queue == null) throw new ArgumentNullException(nameof(result), "缺少队列信息");
        if (result.Queue.BrokerName.IsNullOrEmpty()) throw new ArgumentNullException(nameof(result), "缺少BrokerName");

        var header = new EndTransactionRequestHeader
        {
            ProducerGroup = Group,
            TranStateTableOffset = result.QueueOffset,
            CommitLogOffset = GetCommitLogOffset(result.OffsetMsgId),
            CommitOrRollback = (Int32)state,
            FromTransactionCheck = fromTransactionCheck,
            MsgId = result.MsgId,
            TransactionId = result.TransactionId,
        };

        var bk = GetBroker(result.Queue.BrokerName);
        bk.Invoke(RequestCode.END_TRANSACTION, null, header.GetProperties());
    }

    /// <summary>异步结束事务消息。提交或回滚事务</summary>
    /// <param name="result">发布事务消息返回结果</param>
    /// <param name="state">事务状态</param>
    /// <param name="fromTransactionCheck">是否来自事务回查</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public virtual async Task EndTransactionAsync(SendResult result, TransactionState state, Boolean fromTransactionCheck = false, CancellationToken cancellationToken = default)
    {
        if (result is null) throw new ArgumentNullException(nameof(result));
        if (result.Queue == null) throw new ArgumentNullException(nameof(result), "缺少队列信息");
        if (result.Queue.BrokerName.IsNullOrEmpty()) throw new ArgumentNullException(nameof(result), "缺少BrokerName");

        var header = new EndTransactionRequestHeader
        {
            ProducerGroup = Group,
            TranStateTableOffset = result.QueueOffset,
            CommitLogOffset = GetCommitLogOffset(result.OffsetMsgId),
            CommitOrRollback = (Int32)state,
            FromTransactionCheck = fromTransactionCheck,
            MsgId = result.MsgId,
            TransactionId = result.TransactionId,
        };

        var bk = GetBroker(result.Queue.BrokerName);
        await bk.InvokeAsync(RequestCode.END_TRANSACTION, null, header.GetProperties(), false, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region 辅助
    /// <summary>
    /// 创建消息，设计于支持用户重载以改变消息序列化行为
    /// </summary>
    /// <param name="body"></param>
    /// <returns></returns>
    protected virtual Message CreateMessage(Object body)
    {
        if (body is null) throw new ArgumentNullException(nameof(body));
        if (body is Message) throw new ArgumentOutOfRangeException(nameof(body), "body不能是Message类型");

        if (!body.GetType().IsBaseType()) body = JsonHost.Write(body);

        var msg = new Message();
        msg.SetBody(body);

        return msg;
    }

    private SendMessageRequestHeader CreateHeader(Message message)
    {
        var max = MaxMessageSize;
        if (max > 0 && message.Body.Length > max) throw new InvalidOperationException($"主题[{Topic}]的数据包大小[{message.Body.Length}]超过最大限制[{max}]，大key会拖累整个队列，可通过MaxMessageSize调节。");

        // 消息压缩
        var sysFlag = 0;
        var compressOver = CompressOverBytes;
        if (compressOver > 0 && message.Body != null && message.Body.Length > compressOver)
        {
            message.Body = message.Body.Compress();
            sysFlag |= 1; // 第0位表示压缩
        }

        // 构造请求头
        var smrh = new SendMessageRequestHeader
        {
            ProducerGroup = Group,
            Topic = Topic,
            //QueueId = mq.QueueId,
            SysFlag = sysFlag,
            BornTimestamp = DateTime.UtcNow.ToLong(),
            Flag = message.Flag,
            Properties = message.GetProperties(),
            ReconsumeTimes = 0,
            UnitMode = UnitMode,
            DefaultTopic = DefaultTopic,
            DefaultTopicQueueNums = DefaultTopicQueueNums
        };

        if (message.Properties.TryGetValue("TRAN_MSG", out var str) && str.ToBoolean()) smrh.SysFlag = (Int32)TransactionState.Prepared;

        return smrh;
    }

    private static Int64 GetCommitLogOffset(String offsetMsgId)
    {
        if (offsetMsgId.IsNullOrEmpty() || offsetMsgId.Length < CommitLogOffsetHexLength) return 0;

        // OffsetMsgId尾部16位是8字节（Int64）的CommitLogOffset十六进制表示
        return Int64.TryParse(offsetMsgId.Substring(offsetMsgId.Length - CommitLogOffsetHexLength), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var rs) ? rs : 0;
    }
    #endregion

    #region 选择Broker队列
    private IList<BrokerInfo> _brokers;
    //private WeightRoundRobin _robin;
    /// <summary>选择队列</summary>
    /// <returns></returns>
    public virtual MessageQueue SelectQueue()
    {
        var lb = LoadBalance;
        if (!lb.Ready)
        {
            // 只选择主节点且可写
            var list = Brokers.Where(e => e.IsMaster && e.Permission.HasFlag(Permissions.Write) && e.WriteQueueNums > 0).ToList();
            if (list.Count == 0) return null;

            var total = list.Sum(e => e.WriteQueueNums);
            if (total <= 0) return null;

            _brokers = list;
            //lb = new WeightRoundRobin();
            lb.Set(list.Select(e => e.WriteQueueNums).ToArray());
        }

        // 解锁解决冲突，让消息发送更均匀
        lock (lb)
        {
            // 构造排序列表。希望能够均摊到各Broker
            var idx = lb.Get(out var times);
            var bk = _brokers[idx];
            return new MessageQueue { BrokerName = bk.Name, QueueId = (times - 1) % bk.WriteQueueNums };
        }
    }
    #endregion

    #region Request-Reply 请求响应模式
    /// <summary>发送请求消息，同步等待响应</summary>
    /// <param name="message">请求消息</param>
    /// <param name="timeout">超时时间(毫秒)，默认使用RequestTimeout</param>
    /// <returns>响应消息</returns>
    public virtual MessageExt Request(Message message, Int32 timeout = -1)
    {
        if (message == null) throw new ArgumentNullException(nameof(message));

        if (timeout <= 0) timeout = RequestTimeout;

        // 生成关联ID
        var correlationId = Guid.NewGuid().ToString("N");
        message.CorrelationId = correlationId;
        message.ReplyToClient = ClientId;
        message.MessageType = "REQUEST";
        message.RequestTimeout = timeout;

        // 注册回调
        var tcs = new TaskCompletionSource<MessageExt>();
        _requestCallbacks[correlationId] = tcs;

        try
        {
            // 发送请求消息
            var result = Publish(message, null, timeout);

            // 等待响应
            if (tcs.Task.Wait(timeout))
            {
                return tcs.Task.Result;
            }
            else
            {
                throw new TimeoutException($"Request timeout after {timeout}ms, correlationId={correlationId}");
            }
        }
        finally
        {
            // 清理回调
            _requestCallbacks.TryRemove(correlationId, out _);
        }
    }

    /// <summary>发送请求消息，同步等待响应</summary>
    /// <param name="body">消息体</param>
    /// <param name="timeout">超时时间(毫秒)</param>
    /// <returns>响应消息</returns>
    public virtual MessageExt Request(Object body, Int32 timeout = -1) => Request(CreateMessage(body), timeout);

    /// <summary>发送请求消息，异步等待响应</summary>
    /// <param name="message">请求消息</param>
    /// <param name="timeout">超时时间(毫秒)，默认使用RequestTimeout</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>响应消息</returns>
    public virtual async Task<MessageExt> RequestAsync(Message message, Int32 timeout = -1, CancellationToken cancellationToken = default)
    {
        if (message == null) throw new ArgumentNullException(nameof(message));

        if (timeout <= 0) timeout = RequestTimeout;

        // 生成关联ID
        var correlationId = Guid.NewGuid().ToString("N");
        message.CorrelationId = correlationId;
        message.ReplyToClient = ClientId;
        message.MessageType = "REQUEST";
        message.RequestTimeout = timeout;

        // 注册回调
        var tcs = new TaskCompletionSource<MessageExt>();
        _requestCallbacks[correlationId] = tcs;

        try
        {
            // 发送请求消息
            await PublishAsync(message, null, cancellationToken).ConfigureAwait(false);

            // 等待响应，使用兼容的方式
            var completedTask = await Task.WhenAny(tcs.Task, Task.Delay(timeout, cancellationToken)).ConfigureAwait(false);
            
            if (completedTask == tcs.Task)
            {
                return await tcs.Task.ConfigureAwait(false);
            }
            else
            {
                throw new TimeoutException($"Request timeout after {timeout}ms, correlationId={correlationId}");
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"Request timeout after {timeout}ms, correlationId={correlationId}");
        }
        finally
        {
            // 清理回调
            _requestCallbacks.TryRemove(correlationId, out _);
        }
    }

    /// <summary>发送请求消息，异步等待响应</summary>
    /// <param name="body">消息体</param>
    /// <param name="timeout">超时时间(毫秒)</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>响应消息</returns>
    public virtual Task<MessageExt> RequestAsync(Object body, Int32 timeout = -1, CancellationToken cancellationToken = default)
        => RequestAsync(CreateMessage(body), timeout, cancellationToken);

    /// <summary>处理回复消息</summary>
    /// <param name="message">回复消息</param>
    internal void HandleReplyMessage(MessageExt message)
    {
        var correlationId = message.CorrelationId;
        if (String.IsNullOrEmpty(correlationId)) return;

        if (_requestCallbacks.TryGetValue(correlationId, out var tcs))
        {
            tcs.TrySetResult(message);
        }
    }
    #endregion

    #region 事务回查
    /// <summary>收到命令</summary>
    /// <param name="cmd"></param>
    protected override Command OnReceive(Command cmd)
    {
        if (!cmd.Reply)
        {
            var code = (RequestCode)cmd.Header.Code;
            if (code == RequestCode.CHECK_TRANSACTION_STATE)
                return HandleCheckTransaction(cmd);
        }

        return null;
    }

    private Command HandleCheckTransaction(Command cmd)
    {
        using var span = Tracer?.NewSpan($"mq:{Name}:CheckTransaction");
        try
        {
            var dic = cmd.Header?.ExtFields;
            var transactionId = dic != null && dic.TryGetValue("transactionId", out var tid) ? tid : null;
            var commitLogOffset = dic != null && dic.TryGetValue("commitLogOffset", out var clo) ? clo.ToLong() : 0L;
            var tranStateTableOffset = dic != null && dic.TryGetValue("tranStateTableOffset", out var tso) ? tso.ToLong() : 0L;
            var msgId = dic != null && dic.TryGetValue("msgId", out var mid) ? mid : null;

            // 从消息体中尝试解析消息
            MessageExt msgExt = null;
            if (cmd.Payload != null)
            {
                var msgs = MessageExt.ReadAll(cmd.Payload);
                msgExt = msgs?.FirstOrDefault();
            }
            msgExt ??= new MessageExt { MsgId = msgId, TransactionId = transactionId };

            // 调用回查委托
            var state = TransactionState.Rollback;
            if (OnCheckTransactionAsync != null)
                state = OnCheckTransactionAsync(msgExt, transactionId, default).ConfigureAwait(false).GetAwaiter().GetResult();
            else if (OnCheckTransaction != null)
                state = OnCheckTransaction(msgExt, transactionId);
            else
            {
                WriteLog("收到事务回查但未设置OnCheckTransaction委托，事务ID={0}，将默认回滚", transactionId);
            }

            // 构造EndTransaction请求
            var header = new EndTransactionRequestHeader
            {
                ProducerGroup = Group,
                TranStateTableOffset = tranStateTableOffset,
                CommitLogOffset = commitLogOffset,
                CommitOrRollback = (Int32)state,
                FromTransactionCheck = true,
                MsgId = msgId,
                TransactionId = transactionId,
            };

            var bk = Clients?.FirstOrDefault();
            bk?.Invoke(RequestCode.END_TRANSACTION, null, header.GetProperties());

        WriteLog("事务回查完成，事务ID={0}，状态={1}", transactionId, state);
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            WriteLog("事务回查处理异常：{0}", ex.Message);
        }

        return null;
    }
    #endregion

#if NETSTANDARD2_1_OR_GREATER
    #region gRPC扩展方法
    /// <summary>通过gRPC协议发送延迟消息（任意时间）。RocketMQ 5.x新特性</summary>
    /// <param name="body">消息体</param>
    /// <param name="deliveryTimestamp">投递时间（UTC或本地时间）</param>
    /// <param name="tag">标签</param>
    /// <param name="keys">消息Key列表</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>发送结果</returns>
    public async Task<SendResult> PublishDelayViaGrpcAsync(
        Object body,
        DateTime deliveryTimestamp,
        String tag = null,
        IList<String> keys = null,
        CancellationToken cancellationToken = default)
    {
        if (_GrpcService == null) throw new InvalidOperationException("gRPC service not initialized. Set GrpcProxyAddress first.");

        var message = CreateMessage(body);
        using var span = Tracer?.NewSpan($"mq:{Name}:PublishDelay:grpc", new { deliveryTimestamp, body = message.BodyString });
        try
        {
            var rs = await _GrpcService.SendMessageAsync(
                Topic,
                message.Body,
                tag: tag,
                keys: keys,
                deliveryTimestamp: deliveryTimestamp,
                cancellationToken: cancellationToken
            ).ConfigureAwait(false);

            if (rs.Status?.Code != Grpc.GrpcCode.OK)
                throw new InvalidOperationException($"gRPC SendMessage (delay) failed: {rs.Status}");

            var entry = rs.Entries.FirstOrDefault();
            return new SendResult
            {
                Status = SendStatus.SendOK,
                MsgId = entry?.MessageId,
                TransactionId = entry?.TransactionId,
                QueueOffset = entry?.Offset ?? 0,
            };
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }
    }

    /// <summary>通过gRPC协议发送事务消息（半消息）。RocketMQ 5.x gRPC API</summary>
    /// <param name="body">消息体</param>
    /// <param name="tag">标签</param>
    /// <param name="keys">消息Key列表</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>发送结果（包含TransactionId）</returns>
    public async Task<SendResult> PublishTransactionViaGrpcAsync(
        Object body,
        String tag = null,
        IList<String> keys = null,
        CancellationToken cancellationToken = default)
    {
        if (_GrpcService == null) throw new InvalidOperationException("gRPC service not initialized. Set GrpcProxyAddress first.");

        var message = CreateMessage(body);
        using var span = Tracer?.NewSpan($"mq:{Name}:PublishTransaction:grpc", message.BodyString);
        try
        {
            var rs = await _GrpcService.SendTransactionMessageAsync(
                Topic,
                message.Body,
                tag: tag,
                keys: keys,
                cancellationToken: cancellationToken
            ).ConfigureAwait(false);

            if (rs.Status?.Code != Grpc.GrpcCode.OK)
                throw new InvalidOperationException($"gRPC SendTransactionMessage failed: {rs.Status}");

            var entry = rs.Entries.FirstOrDefault();
            return new SendResult
            {
                Status = SendStatus.SendOK,
                MsgId = entry?.MessageId,
                TransactionId = entry?.TransactionId,
                QueueOffset = entry?.Offset ?? 0,
            };
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }
    }

    /// <summary>通过gRPC协议结束事务</summary>
    /// <param name="messageId">消息ID</param>
    /// <param name="transactionId">事务ID</param>
    /// <param name="commit">是否提交。true提交，false回滚</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<Grpc.GrpcEndTransactionResponse> EndTransactionViaGrpcAsync(
        String messageId,
        String transactionId,
        Boolean commit,
        CancellationToken cancellationToken = default)
    {
        if (_GrpcService == null) throw new InvalidOperationException("gRPC service not initialized. Set GrpcProxyAddress first.");

        return await _GrpcService.EndTransactionAsync(
            Topic,
            messageId,
            transactionId,
            commit ? Grpc.GrpcTransactionResolution.COMMIT : Grpc.GrpcTransactionResolution.ROLLBACK,
            cancellationToken
        ).ConfigureAwait(false);
    }

    /// <summary>通过gRPC协议查询主题路由</summary>
    /// <param name="topic">主题名。默认使用当前Topic</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>路由信息</returns>
    public async Task<Grpc.QueryRouteResponse> QueryRouteViaGrpcAsync(String topic = null, CancellationToken cancellationToken = default)
    {
        if (_GrpcService == null) throw new InvalidOperationException("gRPC service not initialized. Set GrpcProxyAddress first.");

        return await _GrpcService.QueryRouteAsync(topic ?? Topic, cancellationToken).ConfigureAwait(false);
    }
    #endregion
#endif
}
