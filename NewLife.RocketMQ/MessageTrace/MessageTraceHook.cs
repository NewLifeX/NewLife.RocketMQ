using System;
using NewLife.RocketMQ.Protocol;
using NewLife.Remoting;
using NewLife.Net;
using NewLife.RocketMQ.MessageTrace;
using NewLife.Log;
using System.Linq;
using NewLife.Data;

namespace NewLife.RocketMQ.MessageTrace
{
    /// <summary>
    /// 消息轨迹钩子实现
    /// </summary>
    internal class MessageTraceHook : ISendMessageHook, IConsumeMessageHook
    {
        private readonly AsyncTraceDispatcher _dispatcher;
        public MessageTraceHook(AsyncTraceDispatcher dispatcher)
        {
            _dispatcher = dispatcher;
        }

        public String HookName => "MessageTraceHook";

        public void ExecuteHookBefore(SendMessageContext context)
        {
            if (context == null) return;
            context.TraceContext = new TraceContext
            {
                TraceType = TraceType.Pub,
                GroupName = context.ProducerGroup,
                RequestId = Guid.NewGuid().ToString("N")
            };
        }

        public void ExecuteHookAfter(SendMessageContext context)
        {

            if (context.Message.Topic.Equals("RMQ_SYS_TRACE_TOPIC"))
            {
                return;
            }
            
            if (context?.SendResult == null || context.SendResult.Status != SendStatus.SendOK) return;

            var traceContext = new TraceContext
            {
                TraceType = TraceType.Pub,
                GroupName = context.ProducerGroup
            };

            var traceBean = new TraceBean
            {
                Topic = context.Message.Topic,
                MsgId = context.SendResult.MsgId,
                Tags = context.Message.Tags,
                Keys = context.Message.Keys,
                StoreHost = context.BrokerAddr,
                BodyLength = context.Message.Body.Length,
                ClientHost = context.Mq.BrokerName,
                MsgType = context.MsgType
            };

            traceContext.TraceBeans.Add(traceBean);
            traceContext.CostTime = (Int32)(DateTime.Now - context.BornHost).TotalMilliseconds;
            _dispatcher.AddTrace(traceContext);
        }

        public void ExecuteHookBefore(ConsumeMessageContext context)
        {
            if (context?.MsgList == null || context.MsgList.Count == 0) return;
            var traceContext = new TraceContext
            {
                TraceType = TraceType.SubBefore,
                GroupName = context.ConsumerGroup,
                RequestId = Guid.NewGuid().ToString("N")
            };
            
            var traceBeans = context.MsgList.Select(msg => new TraceBean
            {
                Topic = msg.Topic,
                MsgId = msg.MsgId,
                Tags = msg.Tags,
                Keys = msg.Keys,
                StoreHost = msg.StoreHost + "",
                BodyLength = msg.Body.Length,
                ClientHost = context.Mq.BrokerName,
                MsgType = context.MsgType
            }).ToList();

            foreach (var traceBean in traceBeans) traceContext.TraceBeans.Add(traceBean);
            _dispatcher.AddTrace(traceContext);
        }

        public void ExecuteHookAfter(ConsumeMessageContext context)
        {
            if (context?.MsgList == null || context.MsgList.Count == 0) return;
            
            var subBeforeTraceContext = context.TraceContext;
            
            var subAfterTraceContext = new TraceContext
            {
                TraceType = TraceType.SubAfter,
                GroupName = context.ConsumerGroup,
                RequestId = subBeforeTraceContext.RequestId,
                Success = context.Success,
            };

            var traceBeans = context.MsgList.Select(msg => new TraceBean
            {
                Topic = msg.Topic,
                MsgId = msg.MsgId,
                Tags = msg.Tags,
                Keys = msg.Keys,
                StoreHost = msg.StoreHost + "",
                BodyLength = msg.Body.Length,
                ClientHost = context.Mq.BrokerName,
                MsgType = context.MsgType
            }).ToList();

            foreach (var traceBean in traceBeans) subAfterTraceContext.TraceBeans.Add(traceBean);
            subAfterTraceContext.CostTime = (Int32)(DateTime.Now - subBeforeTraceContext.TimeStamp).TotalMilliseconds;
            _dispatcher.AddTrace(subAfterTraceContext);
        }
    }
}
