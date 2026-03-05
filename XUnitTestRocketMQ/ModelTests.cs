using System;
using System.ComponentModel;
using NewLife.RocketMQ.Models;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>模型和枚举测试</summary>
public class ModelTests
{
    #region DelayTimeLevels
    [Fact]
    [DisplayName("DelayTimeLevels_包含18个等级")]
    public void DelayTimeLevels_Has18Levels()
    {
        var values = Enum.GetValues(typeof(DelayTimeLevels));

        Assert.Equal(18, values.Length);
    }

    [Fact]
    [DisplayName("DelayTimeLevels_等级值从1开始")]
    public void DelayTimeLevels_StartsFrom1()
    {
        Assert.Equal(1, (Int32)DelayTimeLevels.S1);
        Assert.Equal(2, (Int32)DelayTimeLevels.S5);
        Assert.Equal(18, (Int32)DelayTimeLevels.Hour2);
    }

    [Fact]
    [DisplayName("DelayTimeLevels_关键等级值正确")]
    public void DelayTimeLevels_KeyValues()
    {
        Assert.Equal(5, (Int32)DelayTimeLevels.Min1);
        Assert.Equal(14, (Int32)DelayTimeLevels.Min10);
        Assert.Equal(16, (Int32)DelayTimeLevels.Min30);
        Assert.Equal(17, (Int32)DelayTimeLevels.Hour1);
    }
    #endregion

    #region MessageModels
    [Fact]
    [DisplayName("MessageModels_集群和广播模式")]
    public void MessageModels_Values()
    {
        Assert.Equal(0, (Int32)MessageModels.Clustering);
        Assert.Equal(1, (Int32)MessageModels.Broadcasting);
    }
    #endregion

    #region ConsumeTypes
    [Fact]
    [DisplayName("ConsumeTypes_包含Pull和Push")]
    public void ConsumeTypes_Values()
    {
        Assert.True(Enum.IsDefined(typeof(ConsumeTypes), "Pull"));
        Assert.True(Enum.IsDefined(typeof(ConsumeTypes), "Push"));
    }
    #endregion

    #region ConsumeEventArgs
    [Fact]
    [DisplayName("ConsumeEventArgs_属性可设置")]
    public void ConsumeEventArgs_PropertiesCanBeSet()
    {
        var mq = new MessageQueue { BrokerName = "b1", QueueId = 0 };
        var msgs = new MessageExt[] { new() { Topic = "t1" } };
        var pr = new PullResult { Status = PullStatus.Found };

        var args = new ConsumeEventArgs
        {
            Queue = mq,
            Messages = msgs,
            Result = pr,
        };

        Assert.Same(mq, args.Queue);
        Assert.Single(args.Messages);
        Assert.Equal(PullStatus.Found, args.Result.Status);
    }

    [Fact]
    [DisplayName("ConsumeEventArgs_默认值为null")]
    public void ConsumeEventArgs_Defaults()
    {
        var args = new ConsumeEventArgs();

        Assert.Null(args.Queue);
        Assert.Null(args.Messages);
        Assert.Null(args.Result);
    }
    #endregion

    #region ServiceState
    [Fact]
    [DisplayName("ServiceState_枚举包含基本状态")]
    public void ServiceState_Values()
    {
        Assert.True(Enum.IsDefined(typeof(ServiceState), "CreateJust"));
        Assert.True(Enum.IsDefined(typeof(ServiceState), "Running"));
        Assert.True(Enum.IsDefined(typeof(ServiceState), "ShutdownAlready"));
    }
    #endregion

    #region RequestCode
    [Fact]
    [DisplayName("RequestCode_核心指令码正确")]
    public void RequestCode_CoreValues()
    {
        Assert.Equal(10, (Int32)RequestCode.SEND_MESSAGE);
        Assert.Equal(11, (Int32)RequestCode.PULL_MESSAGE);
        Assert.Equal(12, (Int32)RequestCode.QUERY_MESSAGE);
        Assert.Equal(34, (Int32)RequestCode.HEART_BEAT);
        Assert.Equal(35, (Int32)RequestCode.UNREGISTER_CLIENT);
        Assert.Equal(100, (Int32)RequestCode.PUT_KV_CONFIG);
        Assert.Equal(104, (Int32)RequestCode.UNREGISTER_BROKER);
        Assert.Equal(105, (Int32)RequestCode.GET_ROUTEINTO_BY_TOPIC);
    }
    #endregion

    #region ResponseCode
    [Fact]
    [DisplayName("ResponseCode_核心响应码正确")]
    public void ResponseCode_CoreValues()
    {
        Assert.Equal(0, (Int32)ResponseCode.SUCCESS);
        Assert.Equal(1, (Int32)ResponseCode.SYSTEM_ERROR);
        Assert.Equal(16, (Int32)ResponseCode.NO_PERMISSION);
        Assert.Equal(17, (Int32)ResponseCode.TOPIC_NOT_EXIST);
    }
    #endregion

    #region LanguageCode
    [Fact]
    [DisplayName("LanguageCode_包含主要语言")]
    public void LanguageCode_MainValues()
    {
        Assert.True(Enum.IsDefined(typeof(LanguageCode), "JAVA"));
        Assert.True(Enum.IsDefined(typeof(LanguageCode), "CPP"));
        Assert.True(Enum.IsDefined(typeof(LanguageCode), "DOTNET"));
        Assert.True(Enum.IsDefined(typeof(LanguageCode), "GO"));
    }
    #endregion

    #region TransactionState
    [Fact]
    [DisplayName("TransactionState_枚举值正确")]
    public void TransactionState_Values()
    {
        Assert.Equal(4, (Int32)TransactionState.Prepared);
        Assert.Equal(8, (Int32)TransactionState.Commit);
        Assert.Equal(12, (Int32)TransactionState.Rollback);
    }
    #endregion
}
