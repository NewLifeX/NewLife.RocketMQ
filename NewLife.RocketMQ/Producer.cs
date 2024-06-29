﻿using NewLife.Log;
using NewLife.Reflection;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Common;
using NewLife.RocketMQ.Models;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ;

/// <summary>生产者</summary>
public class Producer : MqBase
{
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
    #endregion

    #region 基础方法
    /// <summary>启动</summary>
    /// <returns></returns>
    protected override void OnStart()
    {
        base.OnStart();

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

            // 性能埋点
            using var span = Tracer?.NewSpan($"mq:{Name}:Publish", message.BodyString);
            span?.AppendTag($"queue={mq}");
            try
            {
                // 根据队列获取Broker客户端
                var bk = GetBroker(mq.BrokerName);
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
    #endregion

    #region 异步发布消息
    /// <summary>发布消息</summary>
    /// <param name="message">消息体</param>
    /// <param name="queue">目标队列。指定时可实现顺序发布（通过SelectQueue获取），默认未指定并自动选择队列</param>
    /// <param name="cancellationToken"></param>
    public virtual async Task<SendResult> PublishAsync(Message message, MessageQueue queue, CancellationToken cancellationToken = default)
    {
        if (message is null) throw new ArgumentNullException(nameof(message));

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
                // 根据队列获取Broker客户端
                var bk = GetBroker(mq.BrokerName);
                var rs = await bk.InvokeAsync(RequestCode.SEND_MESSAGE_V2, message.Body, header.GetProperties(), true, cancellationToken);

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
                    await Task.Delay(TimeSpan.FromSeconds(1));
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

        // 构造请求头
        var smrh = new SendMessageRequestHeader
        {
            ProducerGroup = Group,
            Topic = Topic,
            //QueueId = mq.QueueId,
            SysFlag = 0,
            BornTimestamp = DateTime.UtcNow.ToLong(),
            Flag = message.Flag,
            Properties = message.GetProperties(),
            ReconsumeTimes = 0,
            UnitMode = UnitMode,
            DefaultTopic = "TBW102"
        };

        return smrh;
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
            var list = Brokers.Where(e => e.Permission.HasFlag(Permissions.Write) && e.WriteQueueNums > 0).ToList();
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
}