using System;
using System.ComponentModel;
using System.Reflection;
using NewLife;
using NewLife.RocketMQ;
using NewLife.RocketMQ.MessageTrace;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTest.Trace;

/// <summary>消息轨迹钩子单元测试（钩子为内部类型，通过反射构造）</summary>
public class MessageTraceHookTests
{
    private sealed class HookScope : IDisposable
    {
        private readonly Producer _host;
        private readonly IDisposable _dispatcher;

        public ISendMessageHook Send { get; }
        public IConsumeMessageHook Consume { get; }

        public HookScope()
        {
            var asm = typeof(Producer).Assembly;
            var dispatcherType = asm.GetType("NewLife.RocketMQ.MessageTrace.AsyncTraceDispatcher");
            var hookType = asm.GetType("NewLife.RocketMQ.MessageTrace.MessageTraceHook");
            Assert.NotNull(dispatcherType);
            Assert.NotNull(hookType);

            _host = new Producer();
            _dispatcher = (IDisposable)Activator.CreateInstance(
                dispatcherType,
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                null,
                new Object[] { _host },
                null)!;
            var hook = Activator.CreateInstance(
                hookType,
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                null,
                new Object[] { _dispatcher },
                null)!;

            Send = (ISendMessageHook)hook;
            Consume = (IConsumeMessageHook)hook;
        }

        public void Dispose()
        {
            _dispatcher.Dispose();
            _host.Dispose();
        }
    }

    [Fact]
    [DisplayName("发送前钩子_写入Pub轨迹上下文")]
    public void ExecuteHookBefore_SetsPubTraceContext()
    {
        using var scope = new HookScope();
        var context = new SendMessageContext { ProducerGroup = "test-group" };

        scope.Send.ExecuteHookBefore(context);

        Assert.NotNull(context.TraceContext);
        Assert.Equal(TraceType.Pub, context.TraceContext.TraceType);
        Assert.Equal("test-group", context.TraceContext.GroupName);
        Assert.False(context.TraceContext.RequestId.IsNullOrEmpty());
    }

    [Fact]
    [DisplayName("发送后钩子_发送失败_不抛异常")]
    public void ExecuteHookAfter_FailedSend_NoThrow()
    {
        using var scope = new HookScope();
        var context = new SendMessageContext
        {
            Message = new Message { Topic = "topic1", Body = new Byte[] { 1 } },
            SendResult = new SendResult { Status = SendStatus.FlushDiskTimeout },
        };

        scope.Send.ExecuteHookAfter(context);
    }

    [Fact]
    [DisplayName("发送后钩子_轨迹主题自身_被忽略")]
    public void ExecuteHookAfter_TraceTopic_Ignored()
    {
        using var scope = new HookScope();
        var context = new SendMessageContext
        {
            Message = new Message { Topic = "RMQ_SYS_TRACE_TOPIC", Body = new Byte[] { 1 } },
            SendResult = new SendResult { Status = SendStatus.SendOK, MsgId = "trace-msg" },
        };

        scope.Send.ExecuteHookAfter(context);
    }

    [Fact]
    [DisplayName("发送后钩子_发送成功_构建轨迹不抛异常")]
    public void ExecuteHookAfter_Success_NoThrow()
    {
        using var scope = new HookScope();
        var context = new SendMessageContext
        {
            ProducerGroup = "test-group",
            Message = new Message { Topic = "topic1", Body = new Byte[] { 1, 2, 3 }, Tags = "TagA", Keys = "k1" },
            Mq = new MessageQueue { Topic = "topic1", BrokerName = "broker-a", QueueId = 0 },
            BrokerAddr = "127.0.0.1:10911",
            SendResult = new SendResult { Status = SendStatus.SendOK, MsgId = "msg-1" },
            BornHost = DateTime.Now.AddMilliseconds(-5),
            MsgType = "NORMAL",
        };

        scope.Send.ExecuteHookAfter(context);
    }

    [Fact]
    [DisplayName("消费前后钩子_非空消息列表_不抛异常")]
    public void ExecuteConsumeHooks_WithMessages_NoThrow()
    {
        using var scope = new HookScope();
        var context = new ConsumeMessageContext
        {
            ConsumerGroup = "test-group",
            MsgList =
            [
                new MessageExt
                {
                    Topic = "topic1",
                    MsgId = "msg-1",
                    Body = new Byte[] { 1, 2 },
                    Tags = "TagA",
                    Keys = "k1",
                },
            ],
            Mq = new MessageQueue { Topic = "topic1", BrokerName = "broker-a", QueueId = 0 },
            MsgType = "NORMAL",
            Success = true,
            TraceContext = new TraceContext { RequestId = "request-1", TimeStamp = DateTime.Now.AddMilliseconds(-10) },
        };

        scope.Consume.ExecuteHookBefore(context);
        scope.Consume.ExecuteHookAfter(context);
    }

    [Fact]
    [DisplayName("消费前后钩子_空消息列表_直接返回")]
    public void ExecuteConsumeHooks_EmptyMessages_NoThrow()
    {
        using var scope = new HookScope();
        var context = new ConsumeMessageContext { MsgList = [], TraceContext = new TraceContext() };

        scope.Consume.ExecuteHookBefore(context);
        scope.Consume.ExecuteHookAfter(context);
    }
}
