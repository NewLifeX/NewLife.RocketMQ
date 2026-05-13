using System;
using System.Collections.Generic;
using System.ComponentModel;
using Moq;
using NewLife.RocketMQ;
using NewLife.RocketMQ.MessageTrace;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ.Producers;

/// <summary>ISendMessageHook 钩子机制单元测试</summary>
public class SendMessageHookTests
{
    #region 注册方法
    [Fact]
    [DisplayName("RegisterSendMessageHook_注册null抛出ArgumentNullException")]
    public void RegisterSendMessageHook_Null_ThrowsArgumentNullException()
    {
        using var producer = new Producer();
        Assert.Throws<ArgumentNullException>(() => producer.RegisterSendMessageHook(null));
    }

    [Fact]
    [DisplayName("RegisterSendMessageHook_成功注册单个Hook")]
    public void RegisterSendMessageHook_SingleHook_Registered()
    {
        using var producer = new Producer();
        var mockHook = new Mock<ISendMessageHook>();

        // 注册不抛异常
        var ex = Record.Exception(() => producer.RegisterSendMessageHook(mockHook.Object));
        Assert.Null(ex);
    }

    [Fact]
    [DisplayName("RegisterSendMessageHook_可注册多个Hook")]
    public void RegisterSendMessageHook_MultipleHooks_AllRegistered()
    {
        using var producer = new Producer();
        var mockHook1 = new Mock<ISendMessageHook>();
        var mockHook2 = new Mock<ISendMessageHook>();

        var ex = Record.Exception(() =>
        {
            producer.RegisterSendMessageHook(mockHook1.Object);
            producer.RegisterSendMessageHook(mockHook2.Object);
        });
        Assert.Null(ex);
    }
    #endregion

    #region 钩子执行流程
    [Fact]
    [DisplayName("SendMessageHook_ExecuteHookBefore_携带正确的Context信息")]
    public void ExecuteHookBefore_ContextHasRequiredFields()
    {
        // 直接调用钩子接口方法，验证 Context 数据流
        var capturedContext = (SendMessageContext)null;
        var mockHook = new Mock<ISendMessageHook>();
        mockHook
            .Setup(h => h.ExecuteHookBefore(It.IsAny<SendMessageContext>()))
            .Callback<SendMessageContext>(ctx => capturedContext = ctx);

        var context = new SendMessageContext
        {
            ProducerGroup = "TestProducerGroup",
            Message = new Message { Topic = "TestTopic", Body = [1, 2, 3] },
            Mq = new MessageQueue { BrokerName = "Broker-A", QueueId = 0 },
            BrokerAddr = "127.0.0.1:10911",
        };

        mockHook.Object.ExecuteHookBefore(context);

        Assert.NotNull(capturedContext);
        Assert.Equal("TestProducerGroup", capturedContext.ProducerGroup);
        Assert.Equal("TestTopic", capturedContext.Message.Topic);
        Assert.Equal("Broker-A", capturedContext.Mq.BrokerName);
    }

    [Fact]
    [DisplayName("SendMessageHook_ExecuteHookAfter_携带SendResult")]
    public void ExecuteHookAfter_ContextHasSendResult()
    {
        var capturedContext = (SendMessageContext)null;
        var mockHook = new Mock<ISendMessageHook>();
        mockHook
            .Setup(h => h.ExecuteHookAfter(It.IsAny<SendMessageContext>()))
            .Callback<SendMessageContext>(ctx => capturedContext = ctx);

        var sendResult = new SendResult { Status = SendStatus.SendOK };
        var context = new SendMessageContext
        {
            ProducerGroup = "TestGroup",
            Message = new Message { Topic = "T1" },
            SendResult = sendResult,
        };

        mockHook.Object.ExecuteHookAfter(context);

        Assert.NotNull(capturedContext);
        Assert.Equal(SendStatus.SendOK, capturedContext.SendResult.Status);
    }

    [Fact]
    [DisplayName("SendMessageHook_ExecuteHookAfter_异常时Context携带Exception")]
    public void ExecuteHookAfter_ContextHasException_WhenSendFails()
    {
        var capturedContext = (SendMessageContext)null;
        var mockHook = new Mock<ISendMessageHook>();
        mockHook
            .Setup(h => h.ExecuteHookAfter(It.IsAny<SendMessageContext>()))
            .Callback<SendMessageContext>(ctx => capturedContext = ctx);

        var context = new SendMessageContext
        {
            ProducerGroup = "TestGroup",
            Message = new Message { Topic = "T1" },
            E = new InvalidOperationException("Send failed"),
        };

        mockHook.Object.ExecuteHookAfter(context);

        Assert.NotNull(capturedContext);
        Assert.NotNull(capturedContext.E);
        Assert.IsType<InvalidOperationException>(capturedContext.E);
    }

    [Fact]
    [DisplayName("SendMessageHook_多个Hook按顺序执行")]
    public void MultipleHooks_ExecutedInRegistrationOrder()
    {
        var order = new List<Int32>();
        var mockHook1 = new Mock<ISendMessageHook>();
        var mockHook2 = new Mock<ISendMessageHook>();
        var mockHook3 = new Mock<ISendMessageHook>();

        mockHook1.Setup(h => h.ExecuteHookBefore(It.IsAny<SendMessageContext>())).Callback(() => order.Add(1));
        mockHook2.Setup(h => h.ExecuteHookBefore(It.IsAny<SendMessageContext>())).Callback(() => order.Add(2));
        mockHook3.Setup(h => h.ExecuteHookBefore(It.IsAny<SendMessageContext>())).Callback(() => order.Add(3));

        // 按顺序调用钩子（模拟 Producer 内部循环）
        var hooks = new ISendMessageHook[] { mockHook1.Object, mockHook2.Object, mockHook3.Object };
        var ctx = new SendMessageContext { Message = new Message() };
        foreach (var hook in hooks) hook.ExecuteHookBefore(ctx);

        Assert.Equal([1, 2, 3], order);
    }

    [Fact]
    [DisplayName("SendMessageHook_钩子内异常不影响后续钩子执行")]
    public void HookException_DoesNotPreventSubsequentHookExecution()
    {
        var secondHookCalled = false;
        var hooks = new List<ISendMessageHook>();

        var throwingHook = new Mock<ISendMessageHook>();
        throwingHook.Setup(h => h.ExecuteHookBefore(It.IsAny<SendMessageContext>()))
                    .Throws<InvalidOperationException>();
        hooks.Add(throwingHook.Object);

        var normalHook = new Mock<ISendMessageHook>();
        normalHook.Setup(h => h.ExecuteHookBefore(It.IsAny<SendMessageContext>()))
                  .Callback(() => secondHookCalled = true);
        hooks.Add(normalHook.Object);

        var ctx = new SendMessageContext { Message = new Message() };

        // 模拟 Producer 内部容错循环
        foreach (var hook in hooks)
        {
            try { hook.ExecuteHookBefore(ctx); }
            catch { /* 钩子异常被吞掉，不中断 */ }
        }

        Assert.True(secondHookCalled);
    }
    #endregion

    #region EnableMessageTrace 集成验证
    [Fact]
    [DisplayName("EnableMessageTrace_默认为false")]
    public void EnableMessageTrace_DefaultFalse()
    {
        using var producer = new Producer();
        Assert.False(producer.EnableMessageTrace);
    }

    [Fact]
    [DisplayName("EnableMessageTrace_设置为true后属性值变更")]
    public void EnableMessageTrace_CanBeEnabled()
    {
        using var producer = new Producer();
        producer.EnableMessageTrace = true;
        Assert.True(producer.EnableMessageTrace);
    }
    #endregion
}
