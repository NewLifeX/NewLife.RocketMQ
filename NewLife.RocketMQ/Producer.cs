using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Common;
using NewLife.RocketMQ.Protocol;
using NewLife.Serialization;
#if !NET4
using TaskEx = System.Threading.Tasks.Task;
#endif

namespace NewLife.RocketMQ
{
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
        public override Boolean Start()
        {
            if (!base.Start()) return false;

            if (LoadBalance == null) LoadBalance = new WeightRoundRobin();

            if (_NameServer != null)
            {
                _NameServer.OnBrokerChange += (s, e) =>
                {
                    _brokers = null;
                    //_robin = null;
                    LoadBalance.Ready = false;
                };
            }

            return true;
        }
        #endregion

        #region 发送消息
        /// <summary>
        /// 用于计算 UnixTime 的辅助，在 .NET 4.5 或之前，不存在 DateTimeOffset.Now.ToUnixTimeMilliseconds() 方法
        /// </summary>
        private static readonly DateTime _dt1970 = new(1970, 1, 1);
        /// <summary>发送消息</summary>
        /// <param name="msg"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public virtual SendResult Publish(Message msg, Int32 timeout = -1)
        {
            var max = MaxMessageSize;
            if (max > 0 && msg.Body.Length > max) throw new InvalidOperationException($"主题[{Topic}]的数据包大小[{msg.Body.Length}]超过最大限制[{max}]，大key会拖累整个队列，可通过MaxMessageSize调节。");

            // 选择队列分片
            var mq = SelectQueue();
            mq.Topic = Topic;

            // 构造请求头
            var ts = DateTime.Now - _dt1970;
            var smrh = new SendMessageRequestHeader
            {
                ProducerGroup = Group,
                Topic = Topic,
                QueueId = mq.QueueId,
                SysFlag = 0,
                BornTimestamp = (Int64)ts.TotalMilliseconds,
                Flag = msg.Flag,
                Properties = msg.GetProperties(),
                ReconsumeTimes = 0,
                UnitMode = UnitMode,
                DefaultTopic = "TBW102"
            };

            for (var i = 0; i <= RetryTimesWhenSendFailed; i++)
            {
                // 性能埋点
                using var span = Tracer?.NewSpan($"mq:{Topic}:Publish");
                try
                {
                    // 根据队列获取Broker客户端
                    var bk = GetBroker(mq.BrokerName);
                    var rs = bk.Invoke(RequestCode.SEND_MESSAGE_V2, msg.Body, smrh.GetProperties(), true);

                    // 包装结果
                    var sr = new SendResult
                    {
                        //Status = SendStatus.SendOK,
                        Queue = mq
                    };
                    sr.Status = (ResponseCode)rs.Header.Code switch
                    {
                        ResponseCode.SUCCESS => SendStatus.SendOK,
                        ResponseCode.FLUSH_DISK_TIMEOUT => SendStatus.FlushDiskTimeout,
                        ResponseCode.FLUSH_SLAVE_TIMEOUT => SendStatus.FlushSlaveTimeout,
                        ResponseCode.SLAVE_NOT_AVAILABLE => SendStatus.SlaveNotAvailable,
                        _ => throw rs.Header.CreateException(),
                    };
                    sr.Read(rs.Header?.ExtFields);

                    return sr;
                }
                catch (Exception ex)
                {
                    span?.SetError(ex, msg);

                    // 如果网络异常，则延迟重发
                    if (ex is not ResponseException && i < RetryTimesWhenSendFailed)
                    {
                        Thread.Sleep(1000);
                        continue;
                    }

                    throw;
                }
            }

            return null;
        }

        /// <summary>发布消息</summary>
        /// <param name="body"></param>
        /// <param name="tags"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public virtual SendResult Publish(Object body, String tags = null, Int32 timeout = -1) => Publish(body, tags, null, timeout);

        /// <summary>发布消息</summary>
        /// <param name="body"></param>
        /// <param name="tags">传null则为空</param>
        /// <param name="keys">传null则为空</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public virtual SendResult Publish(Object body, String tags, String keys, Int32 timeout = -1)
        {
            if (body is not Byte[] buf)
            {
                if (body is not String str) str = body.ToJson();

                buf = str.GetBytes();
            }

            return Publish(new Message { Body = buf, Tags = tags, Keys = keys }, timeout);
        }

        /// <summary>发布消息</summary>
        public virtual async Task<SendResult> PublishAsync(Message message)
        {
            var max = MaxMessageSize;
            if (max > 0 && message.Body.Length > max) throw new InvalidOperationException($"主题[{Topic}]的数据包大小[{message.Body.Length}]超过最大限制[{max}]，大key会拖累整个队列，可通过{nameof(MaxMessageSize)}调节最大允许发送数据包大小。");

            // 选择队列分片
            var mq = SelectQueue();
            mq.Topic = Topic;

            // 构造请求头
            var ts = DateTime.Now - _dt1970;
            var sendMessageRequestHeader = new SendMessageRequestHeader
            {
                ProducerGroup = Group,
                Topic = Topic,
                QueueId = mq.QueueId,
                SysFlag = 0,
                BornTimestamp = (Int64)ts.TotalMilliseconds,
                Flag = message.Flag,
                Properties = message.GetProperties(),
                ReconsumeTimes = 0,
                UnitMode = UnitMode,
				DefaultTopic = "TBW102"
            };

            for (var i = 0; i <= RetryTimesWhenSendFailed; i++)
            {
                // 性能埋点
                using var span = Tracer?.NewSpan($"mq:{Topic}:Publish");
                try
                {
                    // 根据队列获取Broker客户端
                    var bk = GetBroker(mq.BrokerName);
                    var rs = await bk.InvokeAsync(RequestCode.SEND_MESSAGE_V2, message.Body, sendMessageRequestHeader.GetProperties(), ignoreError: true);

                    // 包装结果
                    var sendResult = new SendResult
                    {
                        Queue = mq,
                        Status = (ResponseCode) rs.Header.Code switch
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
                    span?.SetError(ex, message);

                    // 如果网络异常，则延迟重发
                    if (ex is not ResponseException && i < RetryTimesWhenSendFailed)
                    {
                        await TaskEx.Delay(TimeSpan.FromSeconds(1));
                        continue;
                    }

                    throw;
                }
            }

            return null;
        }

        /// <summary>发布消息</summary>
        /// <param name="body"></param>
        /// <param name="tags">传null则为空</param>
        /// <returns></returns>
        public virtual Task<SendResult> PublishAsync(Object body, String tags = null) => PublishAsync(body, tags, keys: null);

        /// <summary>发布消息</summary>
        /// <param name="body"></param>
        /// <param name="tags">传null则为空</param>
        /// <param name="keys">传null则为空</param>
        /// <returns></returns>
        public virtual Task<SendResult> PublishAsync(Object body, String tags, String keys)
        {
            if (body is not Byte[] buf)
            {
                if (body is not String str) str = body.ToJson();

                buf = str.GetBytes();
            }

            var message = new Message { Body = buf, Tags = tags, Keys = keys };
            return PublishAsync(message);
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

            // 构造排序列表。希望能够均摊到各Broker
            var idx = lb.Get(out var times);
            var bk = _brokers[idx];
            return new MessageQueue { BrokerName = bk.Name, QueueId = (times - 1) % bk.WriteQueueNums };
        }
        #endregion
    }
}