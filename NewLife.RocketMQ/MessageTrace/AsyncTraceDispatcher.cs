using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NewLife.Log;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ.MessageTrace
{
    /// <summary>
    /// 异步轨迹分发器
    /// </summary>
    internal class AsyncTraceDispatcher : IDisposable
    {
        private readonly Producer _traceProducer;
        private readonly BlockingCollection<TraceContext> _traceQueue;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly Task _dispatchTask;

        /// <summary>轨迹主题</summary>
        public const String TraceTopic = "RMQ_SYS_TRACE_TOPIC";

        internal AsyncTraceDispatcher()
        {
            // 初始化内部生产者
            _traceProducer = new Producer
            {
                Topic = TraceTopic,
                // 使用一个独特的生产者组
                Group = "T_P_G_RMQ_SYS_TRACE_TOPIC",
                Log = XTrace.Log,
            };

            _traceQueue = new BlockingCollection<TraceContext>();
            _cancellationTokenSource = new CancellationTokenSource();

            // 启动后台任务来处理轨迹消息
            _dispatchTask = Task.Factory.StartNew(Dispatch, TaskCreationOptions.LongRunning);
        }

        public void Start(String nameServerAddress)
        {
            if (nameServerAddress.IsNullOrEmpty()) throw new ArgumentNullException(nameof(nameServerAddress));

            _traceProducer.NameServerAddress = nameServerAddress;
            _traceProducer.Start();
        }

        /// <summary>
        /// 添加轨迹上下文到队列
        /// </summary>
        /// <param name="context"></param>
        public void AddTrace(TraceContext context)
        {
            if (!_traceProducer.Active) return;

            try
            {
                _traceQueue.Add(context, _cancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                // 忽略异常，因为这意味着分发器正在关闭
            }
            catch (Exception ex)
            {
                XTrace.WriteException(ex);
            }
        }

        private void Dispatch()
        {
            while (!_cancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    var context = _traceQueue.Take(_cancellationTokenSource.Token);
                    if (context != null)
                    {
                        ProcessTrace(context);
                    }
                }
                catch (OperationCanceledException)
                {
                    break; // 退出循环
                }
                catch (Exception ex)
                {
                    XTrace.WriteException(ex);
                }
            }
        }

        private void ProcessTrace(TraceContext context)
        {
            var sb = new StringBuilder();
            foreach (var bean in context.TraceBeans)
            {
                sb.Append(bean.Topic).Append("\x01");
                sb.Append(bean.MsgId).Append("\x01");
                sb.Append(bean.Tags).Append("\x01");
                sb.Append(bean.Keys).Append("\x01");
                sb.Append(bean.StoreHost).Append("\x01");
                sb.Append(bean.BodyLength).Append("\x01");
                sb.Append(context.CostTime).Append("\x01");
                sb.Append(context.TraceType).Append("\x02");
            }

            var body = sb.ToString().TrimEnd('\x02');
            var keys = context.TraceBeans.Count > 0 ? context.TraceBeans[0].MsgId : "";

            var msg = new Message
            {
                Topic = TraceTopic,
                Tags = context.TraceType.ToString(),
                Keys = keys,
                Body = Encoding.UTF8.GetBytes(body)
            };

            try
            {
                _traceProducer.Publish(msg,null,3000);
            }
            catch (Exception ex)
            { 
                XTrace.WriteException(ex);
            }
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();
            _dispatchTask.Wait(1000);
            _traceProducer.Stop();
        }
    }
}
