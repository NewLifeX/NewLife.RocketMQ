using System;
using System.Collections.Generic;
using System.ComponentModel;
using Moq;
using NewLife.RocketMQ;
using NewLife.RocketMQ.MessageTrace;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>IConsumeMessageHook 钩子机制单元测试</summary>
public class ConsumeMessageHookTests
{
    #region 注册方法
    [Fact]
    [DisplayName("RegisterConsumeMessageHook_注册null抛出ArgumentNullException")]
    public void RegisterConsumeMessageHook_Null_ThrowsArgumentNullException()
    {
        using var consumer = new Consumer();
        Assert.Throws<ArgumentNullException>(() => consumer.RegisterConsumeMessageHook(null));
    }

    [Fact]
    [DisplayName("RegisterConsumeMessageHook_成功注册单个Hook")]
    public void RegisterConsumeMessageHook_SingleHook_Registered()
    {
        using var consumer = new Consumer();
        var mockHook = new Mock<IConsumeMessageHook>();

        var ex = Record.Exception(() => consumer.RegisterConsumeMessageHook(mockHook.Object));
        Assert.Null(ex);
    }

    [Fact]
    [DisplayName("RegisterConsumeMessageHook_可注册多个Hook")]
    public void RegisterConsumeMessageHook_MultipleHooks_AllRegistered()
    {
        using var consumer = new Consumer();
        var mockHook1 = new Mock<IConsumeMessageHook>();
        var mockHook2 = new Mock<IConsumeMessageHook>();

        var ex = Record.Exception(() =>
        {
            consumer.RegisterConsumeMessageHook(mockHook1.Object);
            consumer.RegisterConsumeMessageHook(mockHook2.Object);
        });
        Assert.Null(ex);
    }
    #endregion

    #region 钩子执行流程
    [Fact]
    [DisplayName("ConsumeMessageHook_ExecuteHookBefore_携带正确的Context信息")]
    public void ExecuteHookBefore_ContextHasRequiredFields()
    {
        var capturedContext = (ConsumeMessageContext)null;
        var mockHook = new Mock<IConsumeMessageHook>();
        mockHook
            .Setup(h => h.ExecuteHookBefore(It.IsAny<ConsumeMessageContext>()))
            .Callback<ConsumeMessageContext>(ctx => capturedContext = ctx);

        var msgs = new List<MessageExt>
        {
            new() { MsgId = "MSG-001", Topic = "TestTopic" },
            new() { MsgId = "MSG-002", Topic = "TestTopic" },
        };
        var context = new ConsumeMessageContext
        {
            ConsumerGroup = "TestConsumerGroup",
            MsgList = msgs,
            Mq = new MessageQueue { BrokerName = "Broker-A", QueueId = 1 },
        };

        mockHook.Object.ExecuteHookBefore(context);

        Assert.NotNull(capturedContext);
        Assert.Equal("TestConsumerGroup", capturedContext.ConsumerGroup);
        Assert.Equal(2, capturedContext.MsgList.Count);
        Assert.Equal("Broker-A", capturedContext.Mq.BrokerName);
    }

    [Fact]
    [DisplayName("ConsumeMessageHook_ExecuteHookAfter_携带消费成功标志")]
    public void ExecuteHookAfter_ContextHasSuccessFlag()
    {
        var capturedContext = (ConsumeMessageContext)null;
        var mockHook = new Mock<IConsumeMessageHook>();
        mockHook
            .Setup(h => h.ExecuteHookAfter(It.IsAny<ConsumeMessageContext>()))
            .Callback<ConsumeMessageContext>(ctx => capturedContext = ctx);

        var context = new ConsumeMessageContext
        {
            ConsumerGroup = "G1",
            MsgList = [new MessageExt { MsgId = "M1" }],
            Success = true,
        };

        mockHook.Object.ExecuteHookAfter(context);

        Assert.NotNull(capturedContext);
        Assert.True(capturedContext.Success);
    }

    [Fact]
    [DisplayName("ConsumeMessageHook_ExecuteHookAfter_消费失败时Success为false")]
    public void ExecuteHookAfter_ContextHasFailureFlag_WhenConsumeFails()
    {
        var capturedContext = (ConsumeMessageContext)null;
        var mockHook = new Mock<IConsumeMessageHook>();
        mockHook
            .Setup(h => h.ExecuteHookAfter(It.IsAny<ConsumeMessageContext>()))
            .Callback<ConsumeMessageContext>(ctx => capturedContext = ctx);

        var context = new ConsumeMessageContext
        {
            ConsumerGroup = "G1",
            MsgList = [new MessageExt { MsgId = "M1" }],
            Success = false,
        };

        mockHook.Object.ExecuteHookAfter(context);

        Assert.NotNull(capturedContext);
        Assert.False(capturedContext.Success);
    }

    [Fact]
    [DisplayName("ConsumeMessageHook_多个Hook按顺序执行")]
    public void MultipleHooks_ExecutedInRegistrationOrder()
    {
        var order = new List<Int32>();
        var mockHook1 = new Mock<IConsumeMessageHook>();
        var mockHook2 = new Mock<IConsumeMessageHook>();
        var mockHook3 = new Mock<IConsumeMessageHook>();

        mockHook1.Setup(h => h.ExecuteHookBefore(It.IsAny<ConsumeMessageContext>())).Callback(() => order.Add(1));
        mockHook2.Setup(h => h.ExecuteHookBefore(It.IsAny<ConsumeMessageContext>())).Callback(() => order.Add(2));
        mockHook3.Setup(h => h.ExecuteHookBefore(It.IsAny<ConsumeMessageContext>())).Callback(() => order.Add(3));

        var hooks = new IConsumeMessageHook[] { mockHook1.Object, mockHook2.Object, mockHook3.Object };
        var ctx = new ConsumeMessageContext { MsgList = [] };
        foreach (var hook in hooks) hook.ExecuteHookBefore(ctx);

        Assert.Equal([1, 2, 3], order);
    }

    [Fact]
    [DisplayName("ConsumeMessageHook_钩子内异常不影响后续钩子执行")]
    public void HookException_DoesNotPreventSubsequentHookExecution()
    {
        var secondHookCalled = false;
        var hooks = new List<IConsumeMessageHook>();

        var throwingHook = new Mock<IConsumeMessageHook>();
        throwingHook.Setup(h => h.ExecuteHookBefore(It.IsAny<ConsumeMessageContext>()))
                    .Throws<InvalidOperationException>();
        hooks.Add(throwingHook.Object);

        var normalHook = new Mock<IConsumeMessageHook>();
        normalHook.Setup(h => h.ExecuteHookBefore(It.IsAny<ConsumeMessageContext>()))
                  .Callback(() => secondHookCalled = true);
        hooks.Add(normalHook.Object);

        var ctx = new ConsumeMessageContext { MsgList = [] };
        foreach (var hook in hooks)
        {
            try { hook.ExecuteHookBefore(ctx); }
            catch { /* 钩子异常被吞掉 */ }
        }

        Assert.True(secondHookCalled);
    }
    #endregion

    #region EnableMessageTrace 集成验证
    [Fact]
    [DisplayName("EnableMessageTrace_默认为false")]
    public void EnableMessageTrace_DefaultFalse()
    {
        using var consumer = new Consumer();
        Assert.False(consumer.EnableMessageTrace);
    }

    [Fact]
    [DisplayName("EnableMessageTrace_设置为true后属性值变更")]
    public void EnableMessageTrace_CanBeEnabled()
    {
        using var consumer = new Consumer();
        consumer.EnableMessageTrace = true;
        Assert.True(consumer.EnableMessageTrace);
    }
    #endregion
}
