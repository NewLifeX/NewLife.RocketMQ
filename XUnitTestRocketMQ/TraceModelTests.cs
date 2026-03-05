using System;
using System.Collections.Generic;
using System.ComponentModel;
using NewLife.RocketMQ.MessageTrace;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>消息轨迹数据模型测试</summary>
public class TraceModelTests
{
    #region TraceContext
    [Fact]
    [DisplayName("TraceContext_默认值")]
    public void TraceContext_Defaults()
    {
        var ctx = new TraceContext();

        Assert.Equal(default, ctx.TraceType);
        Assert.Null(ctx.RegionId);
        Assert.Null(ctx.GroupName);
        Assert.Equal(0, ctx.CostTime);
        Assert.False(ctx.Success);
        Assert.Null(ctx.RequestId);
        Assert.NotNull(ctx.TraceBeans);
        Assert.Empty(ctx.TraceBeans);
    }

    [Fact]
    [DisplayName("TraceContext_设置属性")]
    public void TraceContext_SetProperties()
    {
        var ctx = new TraceContext
        {
            TraceType = TraceType.Pub,
            GroupName = "PG_TEST",
            CostTime = 100,
            Success = true,
            RequestId = "REQ001",
            RegionId = "cn-hangzhou",
        };

        Assert.Equal(TraceType.Pub, ctx.TraceType);
        Assert.Equal("PG_TEST", ctx.GroupName);
        Assert.Equal(100, ctx.CostTime);
        Assert.True(ctx.Success);
        Assert.Equal("REQ001", ctx.RequestId);
        Assert.Equal("cn-hangzhou", ctx.RegionId);
    }

    [Fact]
    [DisplayName("TraceContext_添加TraceBeans")]
    public void TraceContext_AddTraceBeans()
    {
        var ctx = new TraceContext();

        ctx.TraceBeans.Add(new TraceBean { Topic = "t1", MsgId = "M1" });
        ctx.TraceBeans.Add(new TraceBean { Topic = "t2", MsgId = "M2" });

        Assert.Equal(2, ctx.TraceBeans.Count);
        Assert.Equal("t1", ctx.TraceBeans[0].Topic);
        Assert.Equal("M2", ctx.TraceBeans[1].MsgId);
    }
    #endregion

    #region TraceBean
    [Fact]
    [DisplayName("TraceBean_默认值")]
    public void TraceBean_Defaults()
    {
        var bean = new TraceBean();

        Assert.Null(bean.Topic);
        Assert.Null(bean.MsgId);
        Assert.Null(bean.OffsetMsgId);
        Assert.Null(bean.Tags);
        Assert.Null(bean.Keys);
        Assert.Null(bean.StoreHost);
        Assert.Equal(0, bean.BodyLength);
        Assert.Null(bean.ClientHost);
        Assert.Null(bean.MsgType);
        Assert.Equal(0, bean.StoreTime);
    }

    [Fact]
    [DisplayName("TraceBean_设置所有属性")]
    public void TraceBean_SetAllProperties()
    {
        var bean = new TraceBean
        {
            Topic = "test_topic",
            MsgId = "MSG001",
            OffsetMsgId = "OFFSET001",
            Tags = "TagA",
            Keys = "Key1",
            StoreHost = "127.0.0.1:10911",
            BodyLength = 256,
            ClientHost = "192.168.1.1",
            MsgType = "Normal",
            StoreTime = 1000L,
        };

        Assert.Equal("test_topic", bean.Topic);
        Assert.Equal("MSG001", bean.MsgId);
        Assert.Equal("OFFSET001", bean.OffsetMsgId);
        Assert.Equal("TagA", bean.Tags);
        Assert.Equal("Key1", bean.Keys);
        Assert.Equal("127.0.0.1:10911", bean.StoreHost);
        Assert.Equal(256, bean.BodyLength);
        Assert.Equal("192.168.1.1", bean.ClientHost);
        Assert.Equal("Normal", bean.MsgType);
        Assert.Equal(1000L, bean.StoreTime);
    }
    #endregion

    #region TraceType枚举
    [Fact]
    [DisplayName("TraceType_枚举值正确")]
    public void TraceType_EnumValues()
    {
        Assert.Equal(0, (Int32)TraceType.Pub);
        Assert.Equal(1, (Int32)TraceType.SubBefore);
        Assert.Equal(2, (Int32)TraceType.SubAfter);
    }
    #endregion

    #region SendMessageContext
    [Fact]
    [DisplayName("SendMessageContext_字段可设置")]
    public void SendMessageContext_FieldsCanBeSet()
    {
        var msg = new Message { Topic = "test" };
        var mq = new MessageQueue { BrokerName = "broker-a", QueueId = 0 };
        var result = new SendResult { Status = SendStatus.SendOK, MsgId = "M1" };

        var ctx = new SendMessageContext
        {
            ProducerGroup = "PG_TEST",
            Message = msg,
            Mq = mq,
            BrokerAddr = "127.0.0.1:10911",
            SendResult = result,
            MsgType = "Normal",
            BornHost = DateTime.Now,
        };

        Assert.Equal("PG_TEST", ctx.ProducerGroup);
        Assert.Same(msg, ctx.Message);
        Assert.Same(mq, ctx.Mq);
        Assert.Equal("127.0.0.1:10911", ctx.BrokerAddr);
        Assert.Same(result, ctx.SendResult);
        Assert.Equal("Normal", ctx.MsgType);
    }

    [Fact]
    [DisplayName("SendMessageContext_TraceContext可设置")]
    public void SendMessageContext_TraceContextCanBeSet()
    {
        var traceCtx = new TraceContext { TraceType = TraceType.Pub };
        var ctx = new SendMessageContext { TraceContext = traceCtx };

        Assert.Same(traceCtx, ctx.TraceContext);
    }
    #endregion

    #region ConsumeMessageContext
    [Fact]
    [DisplayName("ConsumeMessageContext_字段可设置")]
    public void ConsumeMessageContext_FieldsCanBeSet()
    {
        var msgList = new List<MessageExt> { new() { Topic = "t1" } };
        var mq = new MessageQueue { BrokerName = "b1", QueueId = 0 };

        var ctx = new ConsumeMessageContext
        {
            ConsumerGroup = "CG_TEST",
            MsgList = msgList,
            Mq = mq,
            Success = true,
            MsgType = "Normal",
        };

        Assert.Equal("CG_TEST", ctx.ConsumerGroup);
        Assert.Single(ctx.MsgList);
        Assert.Same(mq, ctx.Mq);
        Assert.True(ctx.Success);
        Assert.Equal("Normal", ctx.MsgType);
    }
    #endregion
}
